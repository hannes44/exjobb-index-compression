/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.MalformedInputException;

import java.io.IOException;
import java.util.Arrays;
import static java.util.Objects.requireNonNull;
import static java.lang.String.format;

import static org.apache.lucene.util.compress.zstd.BitInputStream.peekBits;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.*;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_SYMBOL;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_SYMBOL_COUNT;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.checkArgument;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.put24BitLittleEndian;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.get24BitLittleEndian;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.verify;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.fail;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.mask;

/**
 * Zstandard compression and decompression routines.
 */
public final class ZSTD {

    private ZSTD() {}

    static final int MAX_FRAME_HEADER_SIZE = 14;

    private static final int CHECKSUM_FLAG = 0b100;
    private static final int SINGLE_SEGMENT_FLAG = 0b100000;

    private static final int MINIMUM_LITERALS_SIZE = 63;

    // the maximum table log allowed for literal encoding per RFC 8478, section 4.2.1
    private static final int MAX_HUFFMAN_TABLE_LOG = 11;

    public static short readShort(byte[] buf, int i) {
        return (short) BitUtil.VH_NATIVE_SHORT.get(buf, i);
    }

    public static int readInt(byte[] buf, long i) {
        return (int) BitUtil.VH_NATIVE_INT.get(buf, (int) i);
    }

    public static long readLong(byte[] buf, long i) {
        return (long) BitUtil.VH_NATIVE_LONG.get(buf, (int) i);
    }

    public static void writeShort(byte[] buf, int i, short v) {
        BitUtil.VH_NATIVE_SHORT.set(buf, i, v);
    }

    public static void writeInt(byte[] buf, long i, int v) {
        BitUtil.VH_NATIVE_INT.set(buf, (int) i, v);
    }

    public static void writeLong(byte[] buf, long i, long v) {
        BitUtil.VH_NATIVE_LONG.set(buf, (int) i, v);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }

    public static int maxCompressedLength(int uncompressedSize)
    {
        int result = uncompressedSize + (uncompressedSize >>> 8);

        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }

        return result;
    }

    public static int getDecompressedSize(DataInput input) throws IOException {
        //int baseAddress = ARRAY_BYTE_BASE_OFFSET + offset; REMOVE?
        return (int) returnDecompressedSize(input);
    }

    // visible for testing
    static int writeMagic(final DataOutput outputBase) throws IOException {
        //checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small"); REMOVE?

        outputBase.writeInt(MAGIC_NUMBER);
        return SIZE_OF_INT;
    }

    // visible for testing
    static int writeFrameHeader(final DataOutput outputBase, final long outputAddress, int inputSize, int windowSize) throws IOException {
        //checkArgument(outputLimit - outputAddress >= MAX_FRAME_HEADER_SIZE, "Output buffer too small"); REMOVE?

        long output = outputAddress; // REMOVE?

        int contentSizeDescriptor = 0;
        if (inputSize != -1) {
            contentSizeDescriptor = (inputSize >= 256 ? 1 : 0) + (inputSize >= 65536 + 256 ? 1 : 0);
        }
        int frameHeaderDescriptor = (contentSizeDescriptor << 6) | CHECKSUM_FLAG; // dictionary ID missing

        boolean singleSegment = inputSize != -1 && windowSize >= inputSize;
        if (singleSegment) {
            frameHeaderDescriptor |= SINGLE_SEGMENT_FLAG;
        }

        outputBase.writeByte((byte) frameHeaderDescriptor);
        output++;

        if (!singleSegment) {
            int base = Integer.highestOneBit(windowSize);

            int exponent = 32 - Integer.numberOfLeadingZeros(base) - 1;
            if (exponent < MIN_WINDOW_LOG) {
                throw new IllegalArgumentException("Minimum window size is " + (1 << MIN_WINDOW_LOG));
            }

            int remainder = windowSize - base;
            if (remainder % (base / 8) != 0) {
                throw new IllegalArgumentException("Window size of magnitude 2^" + exponent + " must be multiple of " + (base / 8));
            }

            // mantissa is guaranteed to be between 0-7
            int mantissa = remainder / (base / 8);
            int encoded = ((exponent - MIN_WINDOW_LOG) << 3) | mantissa;

            outputBase.writeByte((byte) encoded);
            output++;
        }

        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    outputBase.writeByte((byte) inputSize);
                    output++;
                }
                break;
            case 1:
                outputBase.writeShort((short) (inputSize - 256));
                output += SIZE_OF_SHORT;
                break;
            case 2:
                outputBase.writeInt(inputSize);
                output += SIZE_OF_INT;
                break;
            default:
                throw new AssertionError();
        }

        return (int) (output - outputAddress);
    }

    // visible for testing
    static int writeChecksum(DataOutput outputBase, byte[] inputBase, int inputSize) throws IOException {
        //checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small"); USE?

        long hash = XxHash64.hash(0, inputBase, 0, inputSize);

        outputBase.writeInt((int) hash);

        return SIZE_OF_INT;
    }

    /**
     * Compresses the input data from {@param input} and writes the compressed data to the {@param output}.
     */
    public static int compress(byte[] input, int inputOffset, int inputLength, DataOutput output, int outputOffset, int maxOutputLength) throws IOException {
        verifyRange(input, inputOffset, inputLength);
        //verifyRange(output, outputOffset, maxOutputLength); REMOVE?

        //long outputAddress = outputOffset; // Not explicitly saving the compressed size in the output buffer anymore (Might be needed for decompression)

        return doCompression(input, inputOffset, inputOffset + inputLength, output, outputOffset, outputOffset + maxOutputLength, CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
    }

    public static int doCompression(byte[] inputBase, long inputAddress, long inputLimit, DataOutput outputBase, long outputAddress, long outputLimit, int compressionLevel) throws IOException {
        int inputSize = (int) (inputLimit - inputAddress);

        CompressionParameters parameters = CompressionParameters.compute(compressionLevel, inputSize);

        long output = outputAddress;

        output += writeMagic(outputBase);
        output += writeFrameHeader(outputBase, output, inputSize, parameters.getWindowSize());
        output += compressFrame(inputBase, inputAddress, inputLimit, outputBase, output, outputLimit, parameters);
        output += writeChecksum(outputBase, inputBase, inputSize);

        return (int) (output - outputAddress);
    }

    private static int compressFrame(byte[] inputBase, long inputAddress, long inputLimit, DataOutput outputBase, long outputAddress, long outputLimit, CompressionParameters parameters) throws IOException {
        int blockSize = parameters.getBlockSize();

        int outputSize = (int) (outputLimit - outputAddress);
        int remaining = (int) (inputLimit - inputAddress);

        long output = outputAddress;
        long input = inputAddress;

        CompressionContext context = new CompressionContext(parameters, inputAddress, remaining);
        do {
            //checkArgument(outputSize >= SIZE_OF_BLOCK_HEADER + MIN_BLOCK_SIZE, "Output buffer too small"); USE?

            boolean lastBlock = blockSize >= remaining;
            blockSize = Math.min(blockSize, remaining);

            int compressedSize = writeCompressedBlock(inputBase, input, blockSize, outputBase, output, outputSize, context, lastBlock);

            input += blockSize;
            remaining -= blockSize;
            output += compressedSize;
            outputSize -= compressedSize;
        }
        while (remaining > 0);

        return (int) (output - outputAddress);
    }

    static int writeCompressedBlock(byte[] inputBase, long input, int blockSize, DataOutput outputBase, long output, int outputSize, CompressionContext context, boolean lastBlock) throws IOException {
        checkArgument(lastBlock || blockSize == context.parameters.getBlockSize(), "Only last block can be smaller than block size");

        ByteBuffersDataOutput compressedBuffer = new ByteBuffersDataOutput();
        int compressedSize = 0;
        if (blockSize > 0) {
            compressedSize = compressBlock(inputBase, input, blockSize, compressedBuffer, output + SIZE_OF_BLOCK_HEADER, outputSize - SIZE_OF_BLOCK_HEADER, context);
        }

        if (compressedSize == 0) { // block is not compressible
            checkArgument(blockSize + SIZE_OF_BLOCK_HEADER <= outputSize, "Output size too small"); // USE?

            int blockHeader = (lastBlock ? 1 : 0) | (RAW_BLOCK << 1) | (blockSize << 3);
            put24BitLittleEndian(outputBase, blockHeader);
            outputBase.writeBytes(inputBase, (int) input, blockSize);
            compressedSize = SIZE_OF_BLOCK_HEADER + blockSize;
        }
        else {
            int blockHeader = (lastBlock ? 1 : 0) | (COMPRESSED_BLOCK << 1) | (compressedSize << 3);
            put24BitLittleEndian(outputBase, blockHeader);
            compressedBuffer.copyTo(outputBase);
            compressedSize += SIZE_OF_BLOCK_HEADER;
        }
        return compressedSize;
    }

    private static int compressBlock(byte[] inputBase, long inputAddress, int inputSize, DataOutput outputBase, long outputAddress, int outputSize, CompressionContext context) throws IOException {
        if (inputSize < MIN_BLOCK_SIZE + SIZE_OF_BLOCK_HEADER + 1) {
            //  don't even attempt compression below a certain input size
            return 0;
        }

        CompressionParameters parameters = context.parameters;
        context.blockCompressionState.enforceMaxDistance(inputAddress + inputSize, parameters.getWindowSize());
        context.sequenceStore.reset();

        int lastLiteralsSize = parameters.getStrategy()
                .getCompressor()
                .compressBlock(inputBase, inputAddress, inputSize, context.sequenceStore, context.blockCompressionState, context.offsets, parameters);

        long lastLiteralsAddress = inputAddress + inputSize - lastLiteralsSize;

        // append [lastLiteralsAddress .. lastLiteralsSize] to sequenceStore literals buffer
        context.sequenceStore.appendLiterals(inputBase, lastLiteralsAddress, lastLiteralsSize);

        // convert length/offsets into codes
        context.sequenceStore.generateCodes();

        long outputLimit = outputAddress + outputSize;
        long output = outputAddress;

        int compressedLiteralsSize = encodeLiterals(
                context.huffmanContext,
                parameters,
                outputBase,
                output,
                (int) (outputLimit - output),
                context.sequenceStore.literalsBuffer,
                context.sequenceStore.literalsLength);
        output += compressedLiteralsSize;

        int compressedSequencesSize = SequenceEncoder.compressSequences(outputBase, output, (int) (outputLimit - output), context.sequenceStore, parameters.getStrategy(), context.sequenceEncodingContext);

        int compressedSize = compressedLiteralsSize + compressedSequencesSize;
        if (compressedSize == 0) {
            // not compressible
            return compressedSize;
        }

        // Check compressibility
        int maxCompressedSize = inputSize - calculateMinimumGain(inputSize, parameters.getStrategy());
        if (compressedSize > maxCompressedSize) {
            return 0; // not compressed
        }

        // confirm repeated offsets and entropy tables
        context.commit();

        return compressedSize;
    }

    private static int encodeLiterals(
            HuffmanCompressionContext context,
            CompressionParameters parameters,
            DataOutput outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize) throws IOException {
        // TODO: move this to Strategy
        boolean bypassCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);
        if (bypassCompression || literalsSize <= MINIMUM_LITERALS_SIZE) {
            return rawLiterals(outputBase, outputSize, literals, literalsSize);
        }

        int headerSize = 3 + (literalsSize >= 1024 ? 1 : 0) + (literalsSize >= 16384 ? 1 : 0);

        checkArgument(headerSize + 1 <= outputSize, "Output buffer too small");

        int[] counts = new int[MAX_SYMBOL_COUNT]; // TODO: preallocate
        Histogram.count(literals, literalsSize, counts);
        int maxSymbol = Histogram.findMaxSymbol(counts, MAX_SYMBOL);
        int largestCount = Histogram.findLargestCount(counts, maxSymbol);

        long literalsAddress = 0;
        if (largestCount == literalsSize) {
            // all bytes in input are equal
            return rleLiterals(outputBase, literals, literalsSize);
        }
        else if (largestCount <= (literalsSize >>> 7) + 4) {
            // heuristic: probably not compressible enough
            return rawLiterals(outputBase, outputSize, literals, literalsSize);
        }

        HuffmanCompressionTable previousTable = context.getPreviousTable();
        HuffmanCompressionTable table;
        int serializedTableSize;
        ByteBuffersDataOutput tempOutput = new ByteBuffersDataOutput();
        boolean reuseTable;

        boolean canReuse = previousTable.isValid(counts, maxSymbol);

        // heuristic: use existing table for small inputs if valid
        // TODO: move to Strategy
        boolean preferReuse = parameters.getStrategy().ordinal() < CompressionParameters.Strategy.LAZY.ordinal() && literalsSize <= 1024;
        if (preferReuse && canReuse) {
            table = previousTable;
            reuseTable = true;
            serializedTableSize = 0;
        }
        else {
            HuffmanCompressionTable newTable = context.borrowTemporaryTable();

            newTable.initialize(
                    counts,
                    maxSymbol,
                    HuffmanCompressionTable.optimalNumberOfBits(MAX_HUFFMAN_TABLE_LOG, literalsSize, maxSymbol),
                    context.getCompressionTableWorkspace());
            serializedTableSize = newTable.write(tempOutput, outputAddress + headerSize, outputSize - headerSize, context.getTableWriterWorkspace());

            // Check if using previous huffman table is beneficial
            if (canReuse && previousTable.estimateCompressedSize(counts, maxSymbol) <= serializedTableSize + newTable.estimateCompressedSize(counts, maxSymbol)) {
                table = previousTable;
                reuseTable = true;
                serializedTableSize = 0;
                context.discardTemporaryTable();
            }
            else {
                table = newTable;
                reuseTable = false;
            }
        }

        int compressedSize;
        boolean singleStream = literalsSize < 256;
        if (singleStream) {
            compressedSize = HuffmanCompressor.compressSingleStream(tempOutput, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
        }
        else {
            compressedSize = HuffmanCompressor.compress4streams(tempOutput, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
        }

        int totalSize = serializedTableSize + compressedSize;
        int minimumGain = calculateMinimumGain(literalsSize, parameters.getStrategy());

        if (compressedSize == 0 || totalSize >= literalsSize - minimumGain) {
            // incompressible or no savings

            // discard any temporary table we might have borrowed above
            context.discardTemporaryTable();

            return rawLiterals(outputBase, outputSize, literals, literalsSize);
        }

        int encodingType = reuseTable ? TREELESS_LITERALS_BLOCK : COMPRESSED_LITERALS_BLOCK;

        // Build header
        switch (headerSize) {
            case 3: { // 2 - 2 - 10 - 10
                int header = encodingType | ((singleStream ? 0 : 1) << 2) | (literalsSize << 4) | (totalSize << 14);
                put24BitLittleEndian(outputBase, header);
                break;
            }
            case 4: { // 2 - 2 - 14 - 14
                int header = encodingType | (2 << 2) | (literalsSize << 4) | (totalSize << 18);
                outputBase.writeInt(header);
                break;
            }
            case 5: { // 2 - 2 - 18 - 18
                int header = encodingType | (3 << 2) | (literalsSize << 4) | (totalSize << 22);
                outputBase.writeInt(header);
                outputBase.writeByte((byte) (totalSize >>> 10));
                break;
            }
            default:  // not possible : headerSize is {3,4,5}
                throw new IllegalStateException();
        }
        tempOutput.copyTo(outputBase);
        return headerSize + totalSize;
    }

    private static int rleLiterals(DataOutput outputBase, byte[] inputBase, int inputSize) throws IOException {
        int headerSize = 1 + (inputSize > 31 ? 1 : 0) + (inputSize > 4095 ? 1 : 0);

        switch (headerSize) {
            case 1: // 2 - 1 - 5
                outputBase.writeByte((byte) (RLE_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2: // 2 - 2 - 12
                outputBase.writeShort((short) (RLE_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3: // 2 - 2 - 20
                outputBase.writeInt(RLE_LITERALS_BLOCK | 3 << 2 | inputSize << 4);
                break;
            default:   // impossible. headerSize is {1,2,3}
                throw new IllegalStateException();
        }

        outputBase.writeByte(inputBase[0]);

        return headerSize + 1;
    }

    private static int calculateMinimumGain(int inputSize, CompressionParameters.Strategy strategy)
    {
        // TODO: move this to Strategy to avoid hardcoding a specific strategy here
        int minLog = strategy == CompressionParameters.Strategy.BTULTRA ? 7 : 6;
        return (inputSize >>> minLog) + 2;
    }

    private static int rawLiterals(DataOutput outputBase, int outputSize, byte[] inputBase, int inputSize) throws IOException {
        int headerSize = 1;
        if (inputSize >= 32) {
            headerSize++;
        }
        if (inputSize >= 4096) {
            headerSize++;
        }

        checkArgument(inputSize + headerSize <= outputSize, "Output buffer too small"); // USE?

        switch (headerSize) {
            case 1:
                outputBase.writeByte((byte) (RAW_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2:
                outputBase.writeShort((short) (RAW_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3:
                put24BitLittleEndian(outputBase, RAW_LITERALS_BLOCK | (3 << 2) | (inputSize << 4));
                break;
            default:
                throw new AssertionError();
        }

        // TODO: ensure this test is correct
        checkArgument(inputSize + 1 <= outputSize, "Output buffer too small"); // USE?

        outputBase.writeBytes(inputBase, inputSize);

        return headerSize + inputSize;
    }

    /** Decompresses the input data from {@param input} and writes the decompressed data to the {@param output}. */
    public static int decompress(DataInput input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException, IOException {
        //verifyRange(input, inputOffset, inputLength); REMOVE?
        //verifyRange(output, outputOffset, maxOutputLength); REMOVE?

        long inputLimit = inputOffset + inputLength;
        long outputLimit = outputOffset + maxOutputLength;

        return doDecompression(input, inputOffset, inputLimit, output, outputOffset, outputLimit);
    }

    private static final int[] DEC_32_TABLE = {4, 1, 2, 1, 4, 4, 4, 4};
    private static final int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private static final int V07_MAGIC_NUMBER = 0xFD2FB527;

    static final int MAX_WINDOW_SIZE = 1 << 23;

    private static final int[] LITERALS_LENGTH_BASE = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 0x80, 0x100, 0x200, 0x400, 0x800, 0x1000,
            0x2000, 0x4000, 0x8000, 0x10000};

    private static final int[] MATCH_LENGTH_BASE = {
            3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
            35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 0x83, 0x103, 0x203, 0x403, 0x803,
            0x1003, 0x2003, 0x4003, 0x8003, 0x10003};

    private static final int[] OFFSET_CODES_BASE = {
            0, 1, 1, 5, 0xD, 0x1D, 0x3D, 0x7D,
            0xFD, 0x1FD, 0x3FD, 0x7FD, 0xFFD, 0x1FFD, 0x3FFD, 0x7FFD,
            0xFFFD, 0x1FFFD, 0x3FFFD, 0x7FFFD, 0xFFFFD, 0x1FFFFD, 0x3FFFFD, 0x7FFFFD,
            0xFFFFFD, 0x1FFFFFD, 0x3FFFFFD, 0x7FFFFFD, 0xFFFFFFD};

    private static final FiniteStateEntropy.Table DEFAULT_LITERALS_LENGTH_TABLE = new FiniteStateEntropy.Table(
            6,
            new int[] {
                    0, 16, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 32, 0, 0, 32, 0, 32, 0, 32, 0, 0, 32, 0, 32, 0, 32, 0, 0, 16, 32, 0, 0, 48, 16, 32, 32, 32,
                    32, 32, 32, 32, 32, 0, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0},
            new byte[] {
                    0, 0, 1, 3, 4, 6, 7, 9, 10, 12, 14, 16, 18, 19, 21, 22, 24, 25, 26, 27, 29, 31, 0, 1, 2, 4, 5, 7, 8, 10, 11, 13, 16, 17, 19, 20, 22, 23, 25, 25, 26, 28, 30, 0,
                    1, 2, 3, 5, 6, 8, 9, 11, 12, 15, 17, 18, 20, 21, 23, 24, 35, 34, 33, 32},
            new byte[] {
                    4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 5, 5, 5, 5, 5, 5, 4, 4, 5, 6, 6, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5,
                    6, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6});

    private static final FiniteStateEntropy.Table DEFAULT_OFFSET_CODES_TABLE = new FiniteStateEntropy.Table(
            5,
            new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 16, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0},
            new byte[] {0, 6, 9, 15, 21, 3, 7, 12, 18, 23, 5, 8, 14, 20, 2, 7, 11, 17, 22, 4, 8, 13, 19, 1, 6, 10, 16, 28, 27, 26, 25, 24},
            new byte[] {5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 4, 5, 5, 5, 5, 4, 5, 5, 5, 4, 5, 5, 5, 5, 5, 5, 5});

    private static final FiniteStateEntropy.Table DEFAULT_MATCH_LENGTH_TABLE = new FiniteStateEntropy.Table(
            6,
            new int[] {
                    0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 32, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 48, 16, 32, 32, 32, 32,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
            new byte[] {
                    0, 1, 2, 3, 5, 6, 8, 10, 13, 16, 19, 22, 25, 28, 31, 33, 35, 37, 39, 41, 43, 45, 1, 2, 3, 4, 6, 7, 9, 12, 15, 18, 21, 24, 27, 30, 32, 34, 36, 38, 40, 42, 44, 1,
                    1, 2, 4, 5, 7, 8, 11, 14, 17, 20, 23, 26, 29, 52, 51, 50, 49, 48, 47, 46},
            new byte[] {
                    6, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6,
                    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6});

    private static final byte[] literals = new byte[MAX_BLOCK_SIZE + SIZE_OF_LONG]; // extra space to allow for long-at-a-time copy

    // current buffer containing literals
    private static byte[] literalsBase;
    private static int literalsAddress;
    private static long literalsLimit;

    private static final int[] previousOffsets = new int[3];

    private static final FiniteStateEntropy.Table literalsLengthTable = new FiniteStateEntropy.Table(LITERAL_LENGTH_TABLE_LOG);
    private static final FiniteStateEntropy.Table offsetCodesTable = new FiniteStateEntropy.Table(OFFSET_TABLE_LOG);
    private static final FiniteStateEntropy.Table matchLengthTable = new FiniteStateEntropy.Table(MATCH_LENGTH_TABLE_LOG);

    private static FiniteStateEntropy.Table currentLiteralsLengthTable;
    private static FiniteStateEntropy.Table currentOffsetCodesTable;
    private static FiniteStateEntropy.Table currentMatchLengthTable;

    private static final Huffman huffman = new Huffman();
    private static final FseTableReader fse = new FseTableReader();

    public static int doDecompression(
            final DataInput inputBase,
            final int inputAddress,
            final long inputLimit,
            final byte[] outputBase,
            final int outputAddress,
            final long outputLimit) throws IOException {
        if (outputAddress == outputLimit) {
            return 0;
        }

        int input = inputAddress;
        int output = outputAddress;

        byte[] originalInput = new byte[(int) inputLimit];
        inputBase.clone().readBytes(originalInput, 0, (int) inputLimit);

        while (input < inputLimit) {
            reset();
            long outputStart = output;
            verifyMagic(inputBase, input);
            input += SIZE_OF_INT;

            FrameHeader frameHeader = readFrameHeader(inputBase, input);
            input += (int) frameHeader.headerSize;

            boolean lastBlock;
            do {
                verify(input + SIZE_OF_BLOCK_HEADER <= inputLimit, input, "Not enough input bytes"); // USE?

                // read block header
                int header = get24BitLittleEndian(inputBase, input);
                input += SIZE_OF_BLOCK_HEADER;

                lastBlock = (header & 1) != 0;
                int blockType = (header >>> 1) & 0b11;
                int blockSize = (header >>> 3) & 0x1F_FFFF; // 21 bits

                int decodedSize;
                switch (blockType) {
                    case RAW_BLOCK:
                        verify(input + blockSize <= inputLimit, input, "Not enough input bytes");
                        decodedSize = decodeRawBlock(inputBase, input, blockSize, outputBase, output, outputLimit);
                        input += blockSize;
                        break;
                    case RLE_BLOCK:
                        verify(input + 1 <= inputLimit, input, "Not enough input bytes");
                        decodedSize = decodeRleBlock(blockSize, inputBase, input, outputBase, output, outputLimit);
                        input += 1;
                        break;
                    case COMPRESSED_BLOCK:
                        verify(input + blockSize <= inputLimit, input, "Not enough input bytes");
                        decodedSize = decodeCompressedBlock(inputBase, input, blockSize, outputBase, output, outputLimit, frameHeader.windowSize, outputAddress, originalInput);
                        input += blockSize;
                        break;
                    default:
                        throw fail(input, "Invalid block type");
                }

                output += decodedSize;
            }
            while (!lastBlock);

            if (frameHeader.hasChecksum) {
                int decodedFrameSize = (int) (output - outputStart);

                long hash = XxHash64.hash(0, outputBase, outputStart, decodedFrameSize);

                verify(input + SIZE_OF_INT <= inputLimit, input, "Not enough input bytes");
                int checksum = inputBase.readInt();
                if (checksum != (int) hash) {
                    throw new MalformedInputException(input, format("Bad checksum. Expected: %s, actual: %s", Integer.toHexString(checksum), Integer.toHexString((int) hash)));
                }

                input += SIZE_OF_INT;
            }
        }

        return output - outputAddress;
    }

    static void reset()
    {
        previousOffsets[0] = 1;
        previousOffsets[1] = 4;
        previousOffsets[2] = 8;

        currentLiteralsLengthTable = null;
        currentOffsetCodesTable = null;
        currentMatchLengthTable = null;
    }

    static int decodeRawBlock(DataInput inputBase, long inputAddress, int blockSize, byte[] outputBase, int outputAddress, long outputLimit) throws IOException {
        verify(outputAddress + blockSize <= outputLimit, inputAddress, "Output buffer too small"); // REMOVE?

        //System.arraycopy(inputBase, (int) inputAddress, outputBase, (int) outputAddress, blockSize); Better to use readBytes/writeBytes?? Is this faster?

        byte[] temp = new byte[blockSize];
        inputBase.readBytes(temp, 0, blockSize);
        System.arraycopy(temp, 0, outputBase, outputAddress, blockSize);

        return blockSize;
    }

    static int decodeRleBlock(int size, DataInput inputBase, long inputAddress, byte[] outputBase, int outputAddress, long outputLimit) throws IOException {
        verify(outputAddress + size <= outputLimit, inputAddress, "Output buffer too small");

        int output = outputAddress;
        long value = inputBase.readByte() & 0xFFL;

        int remaining = size;
        if (remaining >= SIZE_OF_LONG) {
            long packed = value
                    | (value << 8)
                    | (value << 16)
                    | (value << 24)
                    | (value << 32)
                    | (value << 40)
                    | (value << 48)
                    | (value << 56);

            do {
                writeLong(outputBase, output, packed);
                output += SIZE_OF_LONG;
                remaining -= SIZE_OF_LONG;
            }
            while (remaining >= SIZE_OF_LONG);
        }

        for (int i = 0; i < remaining; i++) {
            outputBase[output] = (byte) value;
            output++;
        }

        return size;
    }

    @SuppressWarnings("fallthrough")
    static int decodeCompressedBlock(
            DataInput inputBase,
            final int inputAddress,
            int blockSize,
            byte[] outputBase,
            long outputAddress,
            long outputLimit,
            int windowSize,
            long outputAbsoluteBaseAddress,
            byte[] originalInput) throws IOException {
        long inputLimit = inputAddress + blockSize;
        int input = inputAddress;

        verify(blockSize <= MAX_BLOCK_SIZE, input, "Expected match length table to be present");
        verify(blockSize >= MIN_BLOCK_SIZE, input, "Compressed block size too small");

        // decode literals
        int token = inputBase.clone().readByte();
        int literalsBlockType = token & 0b11;
        int type = (token >>> 2) & 0b11;

        switch (literalsBlockType) {
            case RAW_LITERALS_BLOCK: {
                input += decodeRawLiterals(inputBase, input, inputLimit, type, originalInput);
                break;
            }
            case RLE_LITERALS_BLOCK: {
                input += decodeRleLiterals(inputBase, input, blockSize, type, originalInput);
                break;
            }
            case TREELESS_LITERALS_BLOCK:
                verify(huffman.isLoaded(), input, "Dictionary is corrupted");
            case COMPRESSED_LITERALS_BLOCK: {
                input += decodeCompressedLiterals(inputBase, input, blockSize, literalsBlockType, type, originalInput);
                break;
            }
            default:
                throw fail(input, "Invalid literals block encoding type");
        }

        verify(windowSize <= MAX_WINDOW_SIZE, input, "Window size too large (not yet supported)");

        return decompressSequences(
                inputBase, input, inputAddress + blockSize,
                outputBase, outputAddress, outputLimit,
                literalsBase, literalsAddress, literalsLimit,
                outputAbsoluteBaseAddress, originalInput);
    }

    private static int decompressSequences(
            final DataInput inputBase, final long inputAddress, final long inputLimit,
            final byte[] outputBase, final long outputAddress, final long outputLimit,
            final byte[] literalsBase, final long literalsAddress, final long literalsLimit,
            long outputAbsoluteBaseAddress, byte[] originalInput) throws IOException {
        final long fastOutputLimit = outputLimit - SIZE_OF_LONG;
        final long fastMatchOutputLimit = fastOutputLimit - SIZE_OF_LONG;

        long input = inputAddress;
        long output = outputAddress;

        long literalsInput = literalsAddress;

        int size = (int) (inputLimit - inputAddress);
        verify(size >= MIN_SEQUENCES_SIZE, input, "Not enough input bytes");

        // decode header
        int sequenceCount = inputBase.readByte() & 0xFF;
        input++;
        if (sequenceCount != 0) {
            if (sequenceCount == 255) {
                verify(input + SIZE_OF_SHORT <= inputLimit, input, "Not enough input bytes");
                sequenceCount = (inputBase.readShort() & 0xFFFF) + LONG_NUMBER_OF_SEQUENCES;
                input += SIZE_OF_SHORT;
            }
            else if (sequenceCount > 127) {
                verify(input < inputLimit, input, "Not enough input bytes");
                sequenceCount = ((sequenceCount - 128) << 8) + (inputBase.readByte() & 0xFF);
                input++;
            }

            verify(input + SIZE_OF_INT <= inputLimit, input, "Not enough input bytes");

            byte type = inputBase.readByte();
            input++;

            int literalsLengthType = (type & 0xFF) >>> 6;
            int offsetCodesType = (type >>> 4) & 0b11;
            int matchLengthType = (type >>> 2) & 0b11;

            input = computeLiteralsTable(literalsLengthType, inputBase, input, inputLimit, originalInput);
            input = computeOffsetsTable(offsetCodesType, inputBase, input, inputLimit, originalInput);
            input = computeMatchLengthTable(matchLengthType, inputBase, input, inputLimit, originalInput);

            // decompress sequences
            BitInputStream.Initializer initializer = new BitInputStream.Initializer(originalInput, input, inputLimit);
            initializer.initialize();
            int bitsConsumed = initializer.getBitsConsumed();
            long bits = initializer.getBits();
            long currentAddress = initializer.getCurrentAddress();

            int literalsLengthState = (int) peekBits(bitsConsumed, bits, currentLiteralsLengthTable.log2Size);
            bitsConsumed += currentLiteralsLengthTable.log2Size;

            int offsetCodesState = (int) peekBits(bitsConsumed, bits, currentOffsetCodesTable.log2Size);
            bitsConsumed += currentOffsetCodesTable.log2Size;

            int matchLengthState = (int) peekBits(bitsConsumed, bits, currentMatchLengthTable.log2Size);
            bitsConsumed += currentMatchLengthTable.log2Size;


            byte[] literalsLengthNumbersOfBits = currentLiteralsLengthTable.numberOfBits;
            int[] literalsLengthNewStates = currentLiteralsLengthTable.newState;
            byte[] literalsLengthSymbols = currentLiteralsLengthTable.symbol;

            byte[] matchLengthNumbersOfBits = currentMatchLengthTable.numberOfBits;
            int[] matchLengthNewStates = currentMatchLengthTable.newState;
            byte[] matchLengthSymbols = currentMatchLengthTable.symbol;

            byte[] offsetCodesNumbersOfBits = currentOffsetCodesTable.numberOfBits;
            int[] offsetCodesNewStates = currentOffsetCodesTable.newState;
            byte[] offsetCodesSymbols = currentOffsetCodesTable.symbol;

            while (sequenceCount > 0) {
                sequenceCount--;

                BitInputStream.Loader loader = new BitInputStream.Loader(originalInput, input, currentAddress, bits, bitsConsumed);
                loader.load();
                bitsConsumed = loader.getBitsConsumed();
                bits = loader.getBits();
                currentAddress = loader.getCurrentAddress();
                if (loader.isOverflow()) {
                    verify(sequenceCount == 0, input, "Not all sequences were consumed");
                    break;
                }

                // decode sequence
                int literalsLengthCode = literalsLengthSymbols[literalsLengthState];
                int matchLengthCode = matchLengthSymbols[matchLengthState];
                int offsetCode = offsetCodesSymbols[offsetCodesState];

                int literalsLengthBits = LITERALS_LENGTH_BITS[literalsLengthCode];
                int matchLengthBits = MATCH_LENGTH_BITS[matchLengthCode];
                int offsetBits = offsetCode;

                int offset = OFFSET_CODES_BASE[offsetCode];
                if (offsetCode > 0) {
                    offset += (int) peekBits(bitsConsumed, bits, offsetBits);
                    bitsConsumed += offsetBits;
                }

                if (offsetCode <= 1) {
                    if (literalsLengthCode == 0) {
                        offset++;
                    }

                    if (offset != 0) {
                        int temp;
                        if (offset == 3) {
                            temp = previousOffsets[0] - 1;
                        }
                        else {
                            temp = previousOffsets[offset];
                        }

                        if (temp == 0) {
                            temp = 1;
                        }

                        if (offset != 1) {
                            previousOffsets[2] = previousOffsets[1];
                        }
                        previousOffsets[1] = previousOffsets[0];
                        previousOffsets[0] = temp;

                        offset = temp;
                    }
                    else {
                        offset = previousOffsets[0];
                    }
                }
                else {
                    previousOffsets[2] = previousOffsets[1];
                    previousOffsets[1] = previousOffsets[0];
                    previousOffsets[0] = offset;
                }

                int matchLength = MATCH_LENGTH_BASE[matchLengthCode];
                if (matchLengthCode > 31) {
                    matchLength += (int) peekBits(bitsConsumed, bits, matchLengthBits);
                    bitsConsumed += matchLengthBits;
                }

                int literalsLength = LITERALS_LENGTH_BASE[literalsLengthCode];
                if (literalsLengthCode > 15) {
                    literalsLength += (int) peekBits(bitsConsumed, bits, literalsLengthBits);
                    bitsConsumed += literalsLengthBits;
                }

                int totalBits = literalsLengthBits + matchLengthBits + offsetBits;
                if (totalBits > 64 - 7 - (LITERAL_LENGTH_TABLE_LOG + MATCH_LENGTH_TABLE_LOG + OFFSET_TABLE_LOG)) {
                    BitInputStream.Loader loader1 = new BitInputStream.Loader(originalInput, input, currentAddress, bits, bitsConsumed);
                    loader1.load();

                    bitsConsumed = loader1.getBitsConsumed();
                    bits = loader1.getBits();
                    currentAddress = loader1.getCurrentAddress();
                }

                int numberOfBits;

                numberOfBits = literalsLengthNumbersOfBits[literalsLengthState];
                literalsLengthState = (int) (literalsLengthNewStates[literalsLengthState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 9 bits
                bitsConsumed += numberOfBits;

                numberOfBits = matchLengthNumbersOfBits[matchLengthState];
                matchLengthState = (int) (matchLengthNewStates[matchLengthState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 9 bits
                bitsConsumed += numberOfBits;

                numberOfBits = offsetCodesNumbersOfBits[offsetCodesState];
                offsetCodesState = (int) (offsetCodesNewStates[offsetCodesState] + peekBits(bitsConsumed, bits, numberOfBits)); // <= 8 bits
                bitsConsumed += numberOfBits;

                final long literalOutputLimit = output + literalsLength;
                final long matchOutputLimit = literalOutputLimit + matchLength;

                verify(matchOutputLimit <= outputLimit, input, "Output buffer too small");
                long literalEnd = literalsInput + literalsLength;
                verify(literalEnd <= literalsLimit, input, "Input is corrupted");

                long matchAddress = literalOutputLimit - offset;
                verify(matchAddress >= outputAbsoluteBaseAddress, input, "Input is corrupted");

                if (literalOutputLimit > fastOutputLimit) {
                    executeLastSequence(outputBase, output, literalOutputLimit, matchOutputLimit, fastOutputLimit, literalsInput, matchAddress); // TODO: Look if output needs to be used, might be cooked
                }
                else {
                    // copy literals. literalOutputLimit <= fastOutputLimit, so we can copy
                    // long at a time with over-copy
                    output = copyLiterals(outputBase, literalsBase, output, literalsInput, literalOutputLimit);
                    copyMatch(outputBase, fastOutputLimit, output, offset, matchOutputLimit, matchAddress, matchLength, fastMatchOutputLimit);
                }
                output = matchOutputLimit;
                literalsInput = literalEnd;
            }
        }

        // last literal segment
        output = copyLastLiteral(input, literalsBase, literalsInput, literalsLimit, outputBase, output, outputLimit);

        return (int) (output - outputAddress);
    }

    private static long copyLastLiteral(long input, byte[] literalsBase, long literalsInput, long literalsLimit, byte[] outputBase, long output, long outputLimit)
    {
        long lastLiteralsSize = literalsLimit - literalsInput;
        verify(output + lastLiteralsSize <= outputLimit, input, "Output buffer too small");
        System.arraycopy(literalsBase, (int) literalsInput, outputBase, (int) output, (int) lastLiteralsSize);
        output += lastLiteralsSize;
        return output;
    }

    private static void copyMatch(byte[] outputBase,
                                  long fastOutputLimit,
                                  long output,
                                  int offset,
                                  long matchOutputLimit,
                                  long matchAddress,
                                  int matchLength,
                                  long fastMatchOutputLimit)
    {
        matchAddress = copyMatchHead(outputBase, output, offset, matchAddress);
        output += SIZE_OF_LONG;
        matchLength -= SIZE_OF_LONG; // first 8 bytes copied above

        copyMatchTail(outputBase, fastOutputLimit, output, matchOutputLimit, matchAddress, matchLength, fastMatchOutputLimit);
    }

    private static void copyMatchTail(byte[] outputBase, long fastOutputLimit, long output, long matchOutputLimit, long matchAddress, int matchLength, long fastMatchOutputLimit)
    {
        // fastMatchOutputLimit is just fastOutputLimit - SIZE_OF_LONG. It needs to be passed in so that it can be computed once for the
        // whole invocation to decompressSequences. Otherwise, we'd just compute it here.
        // If matchOutputLimit is < fastMatchOutputLimit, we know that even after the head (8 bytes) has been copied, the output pointer
        // will be within fastOutputLimit, so it's safe to copy blindly before checking the limit condition
        if (matchOutputLimit < fastMatchOutputLimit) {
            int copied = 0;
            do {
                writeLong(outputBase, output, readLong(outputBase, matchAddress));
                output += SIZE_OF_LONG;
                matchAddress += SIZE_OF_LONG;
                copied += SIZE_OF_LONG;
            }
            while (copied < matchLength);
        }
        else {
            while (output < fastOutputLimit) {
                writeLong(outputBase, output, readLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            while (output < matchOutputLimit) {
                outputBase[(int) output++] = outputBase[(int) matchAddress++];
            }
        }
    }

    private static long copyMatchHead(byte[] outputBase, long output, int offset, long matchAddress)
    {
        // copy match
        if (offset < 8) {
            // 8 bytes apart so that we can copy long-at-a-time below
            int increment32 = DEC_32_TABLE[offset];
            int decrement64 = DEC_64_TABLE[offset];

            outputBase[(int) output] = outputBase[(int) matchAddress];
            outputBase[(int) output + 1] = outputBase[(int) matchAddress + 1];
            outputBase[(int) output + 2] = outputBase[(int) matchAddress + 2];
            outputBase[(int) output + 3] = outputBase[(int) matchAddress + 3];
            matchAddress += increment32;

            writeInt(outputBase, output + 4, readInt(outputBase, matchAddress));
            matchAddress -= decrement64;
        }
        else {
            writeLong(outputBase, output, readLong(outputBase, matchAddress));
            matchAddress += SIZE_OF_LONG;
        }
        return matchAddress;
    }

    private static long copyLiterals(byte[] outputBase, byte[] literalsBase, long output, long literalsInput, long literalOutputLimit) throws IOException {
        long literalInput = literalsInput;
        do {
            writeLong(outputBase, output, literalsBase[(int) literalInput]);
            output += SIZE_OF_LONG;
            literalInput += SIZE_OF_LONG;
        }
        while (output < literalOutputLimit);
        output = literalOutputLimit; // correction in case we over-copied
        return output;
    }

    private static long computeMatchLengthTable(int matchLengthType, DataInput inputBase, long input, long inputLimit, byte[] originalInput) throws IOException {
        switch (matchLengthType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = inputBase.readByte();
                input++;
                verify(value <= MAX_MATCH_LENGTH_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.initializeRleTable(matchLengthTable, value);
                currentMatchLengthTable = matchLengthTable;
                break;
            case SEQUENCE_ENCODING_BASIC:
                currentMatchLengthTable = DEFAULT_MATCH_LENGTH_TABLE;
                break;
            case SEQUENCE_ENCODING_REPEAT:
                verify(currentMatchLengthTable != null, input, "Expected match length table to be present");
                break;
            case SEQUENCE_ENCODING_COMPRESSED:
                input += fse.readFseTable(matchLengthTable, inputBase, input, inputLimit, MAX_MATCH_LENGTH_SYMBOL, MATCH_LENGTH_TABLE_LOG, originalInput);
                currentMatchLengthTable = matchLengthTable;
                break;
            default:
                throw fail(input, "Invalid match length encoding type");
        }
        return input;
    }

    private static long computeOffsetsTable(int offsetCodesType, DataInput inputBase, long input, long inputLimit, byte[] originalInput) throws IOException {
        switch (offsetCodesType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = inputBase.readByte();
                input++;
                verify(value <= DEFAULT_MAX_OFFSET_CODE_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.initializeRleTable(offsetCodesTable, value);
                currentOffsetCodesTable = offsetCodesTable;
                break;
            case SEQUENCE_ENCODING_BASIC:
                currentOffsetCodesTable = DEFAULT_OFFSET_CODES_TABLE;
                break;
            case SEQUENCE_ENCODING_REPEAT:
                verify(currentOffsetCodesTable != null, input, "Expected match length table to be present");
                break;
            case SEQUENCE_ENCODING_COMPRESSED:
                input += fse.readFseTable(offsetCodesTable, inputBase, input, inputLimit, DEFAULT_MAX_OFFSET_CODE_SYMBOL, OFFSET_TABLE_LOG, originalInput);
                currentOffsetCodesTable = offsetCodesTable;
                break;
            default:
                throw fail(input, "Invalid offset code encoding type");
        }
        return input;
    }

    private static long computeLiteralsTable(int literalsLengthType, DataInput inputBase, long input, long inputLimit, byte[] originalInput) throws IOException {
        switch (literalsLengthType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = inputBase.readByte();
                input++;
                verify(value <= MAX_LITERALS_LENGTH_SYMBOL, input, "Value exceeds expected maximum value");

                FseTableReader.initializeRleTable(literalsLengthTable, value);
                currentLiteralsLengthTable = literalsLengthTable;
                break;
            case SEQUENCE_ENCODING_BASIC:
                currentLiteralsLengthTable = DEFAULT_LITERALS_LENGTH_TABLE;
                break;
            case SEQUENCE_ENCODING_REPEAT:
                verify(currentLiteralsLengthTable != null, input, "Expected match length table to be present");
                break;
            case SEQUENCE_ENCODING_COMPRESSED:
                input += fse.readFseTable(literalsLengthTable, inputBase, input, inputLimit, MAX_LITERALS_LENGTH_SYMBOL, LITERAL_LENGTH_TABLE_LOG, originalInput);
                currentLiteralsLengthTable = literalsLengthTable;
                break;
            default:
                throw fail(input, "Invalid literals length encoding type");
        }
        return input;
    }

    private static void executeLastSequence(byte[] outputBase, long output, long literalOutputLimit, long matchOutputLimit, long fastOutputLimit, long literalInput, long matchAddress)
    {
        // copy literals
        if (output < fastOutputLimit) {
            // wild copy
            do {
                writeLong(outputBase, output, literalsBase[(int) literalInput]);
                output += SIZE_OF_LONG;
                literalInput += SIZE_OF_LONG;
            }
            while (output < fastOutputLimit);

            literalInput -= output - fastOutputLimit;
            output = fastOutputLimit;
        }

        while (output < literalOutputLimit) {
            outputBase[(int)output] = literalsBase[(int) literalInput];
            output++;
            literalInput++;
        }

        // copy match
        while (output < matchOutputLimit) {
            outputBase[(int)output] = outputBase[(int) matchAddress];
            outputBase[(int)output] = literalsBase[(int) matchAddress];
            output++;
            matchAddress++;
        }
    }

    @SuppressWarnings("fallthrough")
    private static int decodeCompressedLiterals(DataInput inputBase, final int inputAddress, int blockSize, int literalsBlockType, int type, byte[] originalInput) throws IOException {
        int input = inputAddress;
        verify(blockSize >= 5, input, "Not enough input bytes");

        // compressed
        int compressedSize;
        int uncompressedSize;
        boolean singleStream = false;
        int headerSize;
        switch (type) {
            case 0:
                singleStream = true;
                // fall through
            case 1: {
                // Cant move input pointer 3 bytes and read correct Int at the same time, so we need to clone the DataInput and read Int from there
                int header = inputBase.clone().readInt();

                // Now we need to move the pointer 3 bytes on the original DataInput
                inputBase.skipBytes(3);

                headerSize = 3;
                uncompressedSize = (header >>> 4) & mask(10);
                compressedSize = (header >>> 14) & mask(10);
                break;
            }
            case 2: {
                int header = inputBase.readInt();

                headerSize = 4;
                uncompressedSize = (header >>> 4) & mask(14);
                compressedSize = (header >>> 18) & mask(14);
                break;
            }
            case 3: {
                // read 5 little-endian bytes
                long header = inputBase.readByte() & 0xFF |
                        (inputBase.readInt() & 0xFFFF_FFFFL) << 8;

                headerSize = 5;
                uncompressedSize = (int) ((header >>> 4) & mask(18));
                compressedSize = (int) ((header >>> 22) & mask(18));
                break;
            }
            default:
                throw fail(input, "Invalid literals header size type");
        }

        verify(uncompressedSize <= MAX_BLOCK_SIZE, input, "Block exceeds maximum size");
        verify(headerSize + compressedSize <= blockSize, input, "Input is corrupted");

        input += headerSize;

        long inputLimit = input + compressedSize;
        if (literalsBlockType != TREELESS_LITERALS_BLOCK) {
            input += huffman.readTable(inputBase, input, compressedSize, originalInput);
        }

        literalsBase = literals;
        literalsAddress = 0;
        literalsLimit = uncompressedSize;

        if (singleStream) {
            huffman.decodeSingleStream(input, inputLimit, literals, literalsAddress, literalsLimit, originalInput);
        }
        else {
            huffman.decode4Streams(inputBase, input, inputLimit, literals, literalsAddress, literalsLimit, originalInput);
        }

        return headerSize + compressedSize;
    }

    private static int decodeRleLiterals(DataInput inputBase, final int inputAddress, int blockSize, int type, byte[] originalInput) throws IOException {
        int input = inputAddress;
        int outputSize;

        switch (type) {
            case 0:
            case 2:
                outputSize = (inputBase.readByte() & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                outputSize = (inputBase.readShort() & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // we need at least 4 bytes (3 for the header, 1 for the payload)
                verify(blockSize >= SIZE_OF_INT, input, "Not enough input bytes");
                outputSize = (inputBase.clone().readInt() & 0xFF_FFFF) >>> 4;
                inputBase.readShort();
                inputBase.readByte();
                input += 3;
                break;
            default:
                throw fail(input, "Invalid RLE literals header encoding type");
        }

        verify(outputSize <= MAX_BLOCK_SIZE, input, "Output exceeds maximum block size");

        byte value = inputBase.readByte();
        Arrays.fill(literals, 0, outputSize + SIZE_OF_LONG, value);

        literalsBase = literals;
        literalsAddress = 0;
        literalsLimit = outputSize;

        return input - inputAddress;
    }

    private static int decodeRawLiterals(DataInput inputBase, final int inputAddress, long inputLimit, int type, byte[] originalInput) throws IOException {
        int input = inputAddress;

        int literalSize;
        switch (type) {
            case 0:
            case 2:
                literalSize = (inputBase.readByte() & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                literalSize = (inputBase.readShort() & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // read 3 little-endian bytes
                int header = ((inputBase.readByte() & 0xFF) |
                        ((inputBase.readShort() & 0xFFFF) << 8));
                literalSize = header >>> 4;
                input += 3;
                break;
            default:
                throw fail(input, "Invalid raw literals header encoding type");
        }

        verify(input + literalSize <= inputLimit, input, "Not enough input bytes");

        // Set literals pointer to [input, literalSize], but only if we can copy 8 bytes at a time during sequence decoding
        // Otherwise, copy literals into buffer that's big enough to guarantee that
        byte[] temp = new byte[literalSize];
        inputBase.readBytes(temp, 0, literalSize);
        if (literalSize > (inputLimit - input) - SIZE_OF_LONG) {
            literalsBase = literals;
            literalsAddress = 0;
            literalsLimit = literalSize;

            System.arraycopy(temp, 0, literals, literalsAddress, literalSize);
            Arrays.fill(literals, literalSize, literalSize + SIZE_OF_LONG, (byte) 0);
        }
        else {
            literalsBase = originalInput;
            literalsAddress = input;
            literalsLimit = literalsAddress + literalSize;
        }
        input += literalSize;

        return input - inputAddress;
    }

    static FrameHeader readFrameHeader(final DataInput inputBase, final int inputAddress) throws IOException {
        int input = inputAddress; // REMOVE?
        //verify(input < inputLimit, input, "Not enough input bytes"); REMOVE?

        int frameHeaderDescriptor = inputBase.readByte() & 0xFF;
        input++;
        boolean singleSegment = (frameHeaderDescriptor & 0b100000) != 0;
        int dictionaryDescriptor = frameHeaderDescriptor & 0b11;
        int contentSizeDescriptor = frameHeaderDescriptor >>> 6;

        int headerSize = 1 +
                (singleSegment ? 0 : 1) +
                (dictionaryDescriptor == 0 ? 0 : (1 << (dictionaryDescriptor - 1))) +
                (contentSizeDescriptor == 0 ? (singleSegment ? 1 : 0) : (1 << contentSizeDescriptor));

        //verify(headerSize <= inputLimit - inputAddress, input, "Not enough input bytes"); USE?

        // decode window size
        int windowSize = -1;
        if (!singleSegment) {
            int windowDescriptor = inputBase.readByte() & 0xFF;
            input++;
            int exponent = windowDescriptor >>> 3;
            int mantissa = windowDescriptor & 0b111;

            int base = 1 << (MIN_WINDOW_LOG + exponent);
            windowSize = base + (base / 8) * mantissa;
        }

        // decode dictionary id
        long dictionaryId = -1;
        switch (dictionaryDescriptor) {
            case 1:
                dictionaryId = inputBase.readByte() & 0xFF;
                input += SIZE_OF_BYTE;
                break;
            case 2:
                dictionaryId = inputBase.readShort() & 0xFFFF;
                input += SIZE_OF_SHORT;
                break;
            case 3:
                dictionaryId = inputBase.readInt() & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
        }
        //verify(dictionaryId == -1, input, "Custom dictionaries not supported"); USE?

        // decode content size
        long contentSize = -1;
        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    contentSize = inputBase.readByte() & 0xFF;
                    input += SIZE_OF_BYTE;
                }
                break;
            case 1:
                contentSize = inputBase.readShort() & 0xFFFF;
                contentSize += 256;
                input += SIZE_OF_SHORT;
                break;
            case 2:
                contentSize = inputBase.readInt() & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
            case 3:
                contentSize = inputBase.readLong();
                input += SIZE_OF_LONG;
                break;
        }

        boolean hasChecksum = (frameHeaderDescriptor & 0b100) != 0;

        return new FrameHeader(
                input - inputAddress,
                windowSize,
                contentSize,
                dictionaryId,
                hasChecksum);
    }

    public static long returnDecompressedSize(final DataInput inputBase) throws IOException {
        // Clone the input so that we can read the magic number without modifying the original input object's file position
        DataInput input = inputBase.clone();
        verifyMagic(input, 0);
        return readFrameHeader(input, 0).contentSize;
    }

    static void verifyMagic(DataInput inputBase, int inputAddress) throws IOException {
        //verify(inputLimit - inputAddress >= 4, inputAddress, "Not enough input bytes"); REMOVE?

        int magic = inputBase.readInt();
        if (magic != MAGIC_NUMBER) {
            if (magic == V07_MAGIC_NUMBER) {
                throw new MalformedInputException(inputAddress, "Data encoded in unsupported ZSTD v0.7 format");
            }
            throw new MalformedInputException(inputAddress, "Invalid magic prefix: " + Integer.toHexString(magic));
        }
    }
}

