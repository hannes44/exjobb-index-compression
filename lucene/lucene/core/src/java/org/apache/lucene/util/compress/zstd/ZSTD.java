/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import java.util.Arrays;
import static java.util.Objects.requireNonNull;
import static java.lang.String.format;

import static org.apache.lucene.util.compress.zstd.BitInputStream.peekBits;
import static org.apache.lucene.util.compress.zstd.Constants.COMPRESSED_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.COMPRESSED_LITERALS_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.DEFAULT_MAX_OFFSET_CODE_SYMBOL;
import static org.apache.lucene.util.compress.zstd.Constants.LITERALS_LENGTH_BITS;
import static org.apache.lucene.util.compress.zstd.Constants.LITERAL_LENGTH_TABLE_LOG;
import static org.apache.lucene.util.compress.zstd.Constants.LONG_NUMBER_OF_SEQUENCES;
import static org.apache.lucene.util.compress.zstd.Constants.MAGIC_NUMBER;
import static org.apache.lucene.util.compress.zstd.Constants.MATCH_LENGTH_BITS;
import static org.apache.lucene.util.compress.zstd.Constants.MATCH_LENGTH_TABLE_LOG;
import static org.apache.lucene.util.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static org.apache.lucene.util.compress.zstd.Constants.MAX_LITERALS_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.zstd.Constants.MAX_MATCH_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.zstd.Constants.MIN_BLOCK_SIZE;
import static org.apache.lucene.util.compress.zstd.Constants.MIN_SEQUENCES_SIZE;
import static org.apache.lucene.util.compress.zstd.Constants.MIN_WINDOW_LOG;
import static org.apache.lucene.util.compress.zstd.Constants.OFFSET_TABLE_LOG;
import static org.apache.lucene.util.compress.zstd.Constants.RAW_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.RAW_LITERALS_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.RLE_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.RLE_LITERALS_BLOCK;
import static org.apache.lucene.util.compress.zstd.Constants.SEQUENCE_ENCODING_BASIC;
import static org.apache.lucene.util.compress.zstd.Constants.SEQUENCE_ENCODING_COMPRESSED;
import static org.apache.lucene.util.compress.zstd.Constants.SEQUENCE_ENCODING_REPEAT;
import static org.apache.lucene.util.compress.zstd.Constants.SEQUENCE_ENCODING_RLE;
import static org.apache.lucene.util.compress.zstd.Constants.SIZE_OF_BLOCK_HEADER;
import static org.apache.lucene.util.compress.zstd.Constants.SIZE_OF_INT;
import static org.apache.lucene.util.compress.zstd.Constants.SIZE_OF_SHORT;
import static org.apache.lucene.util.compress.zstd.Constants.SIZE_OF_LONG;
import static org.apache.lucene.util.compress.zstd.Constants.SIZE_OF_BYTE;
import static org.apache.lucene.util.compress.zstd.Constants.TREELESS_LITERALS_BLOCK;
import static org.apache.lucene.util.compress.zstd.Huffman.MAX_SYMBOL;
import static org.apache.lucene.util.compress.zstd.Huffman.MAX_SYMBOL_COUNT;
import static org.apache.lucene.util.compress.zstd.UnsafeUtil.UNSAFE;
import static org.apache.lucene.util.compress.zstd.Util.checkArgument;
import static org.apache.lucene.util.compress.zstd.Util.put24BitLittleEndian;
import static org.apache.lucene.util.compress.zstd.Util.get24BitLittleEndian;
import static org.apache.lucene.util.compress.zstd.Util.verify;
import static org.apache.lucene.util.compress.zstd.Util.fail;
import static org.apache.lucene.util.compress.zstd.Util.mask;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ZSTD {

    private ZSTD() {}

    static final int MAX_FRAME_HEADER_SIZE = 14;

    private static final int CHECKSUM_FLAG = 0b100;
    private static final int SINGLE_SEGMENT_FLAG = 0b100000;

    private static final int MINIMUM_LITERALS_SIZE = 63;

    // the maximum table log allowed for literal encoding per RFC 8478, section 4.2.1
    private static final int MAX_HUFFMAN_TABLE_LOG = 11;

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

    public static int getDecompressedSize(byte[] input, int offset, int length)
    {
        int baseAddress = ARRAY_BYTE_BASE_OFFSET + offset;
        return (int) returnDecompressedSize(input, baseAddress, baseAddress + length);
    }

    // visible for testing
    static int writeMagic(final Object outputBase, final long outputAddress, final long outputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        UNSAFE.putInt(outputBase, outputAddress, MAGIC_NUMBER);
        return SIZE_OF_INT;
    }

    // visible for testing
    static int writeFrameHeader(final Object outputBase, final long outputAddress, final long outputLimit, int inputSize, int windowSize)
    {
        checkArgument(outputLimit - outputAddress >= MAX_FRAME_HEADER_SIZE, "Output buffer too small");

        long output = outputAddress;

        int contentSizeDescriptor = 0;
        if (inputSize != -1) {
            contentSizeDescriptor = (inputSize >= 256 ? 1 : 0) + (inputSize >= 65536 + 256 ? 1 : 0);
        }
        int frameHeaderDescriptor = (contentSizeDescriptor << 6) | CHECKSUM_FLAG; // dictionary ID missing

        boolean singleSegment = inputSize != -1 && windowSize >= inputSize;
        if (singleSegment) {
            frameHeaderDescriptor |= SINGLE_SEGMENT_FLAG;
        }

        UNSAFE.putByte(outputBase, output, (byte) frameHeaderDescriptor);
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

            UNSAFE.putByte(outputBase, output, (byte) encoded);
            output++;
        }

        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    UNSAFE.putByte(outputBase, output++, (byte) inputSize);
                }
                break;
            case 1:
                UNSAFE.putShort(outputBase, output, (short) (inputSize - 256));
                output += SIZE_OF_SHORT;
                break;
            case 2:
                UNSAFE.putInt(outputBase, output, inputSize);
                output += SIZE_OF_INT;
                break;
            default:
                throw new AssertionError();
        }

        return (int) (output - outputAddress);
    }

    // visible for testing
    static int writeChecksum(Object outputBase, long outputAddress, long outputLimit, Object inputBase, long inputAddress, long inputLimit)
    {
        checkArgument(outputLimit - outputAddress >= SIZE_OF_INT, "Output buffer too small");

        int inputSize = (int) (inputLimit - inputAddress);

        long hash = XxHash64.hash(0, inputBase, inputAddress, inputSize);

        UNSAFE.putInt(outputBase, outputAddress, (int) hash);

        return SIZE_OF_INT;
    }

    /**
     * Compresses the input data from {@param input} and writes the compressed data to the {@param output}.
     */
    public static int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;

        int test = 123456;
        // Write the uncompressed size to the output buffer; For use in decompression
        UNSAFE.putInt(output, outputAddress, test);
        outputAddress += SIZE_OF_INT;

        return SIZE_OF_INT + doCompression(input, inputAddress, inputAddress + inputLength, output, outputAddress, outputAddress + maxOutputLength, CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
    }

    public static int doCompression(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, int compressionLevel)
    {
        int inputSize = (int) (inputLimit - inputAddress);

        CompressionParameters parameters = CompressionParameters.compute(compressionLevel, inputSize);

        long output = outputAddress;

        output += writeMagic(outputBase, output, outputLimit);
        output += writeFrameHeader(outputBase, output, outputLimit, inputSize, parameters.getWindowSize());
        output += compressFrame(inputBase, inputAddress, inputLimit, outputBase, output, outputLimit, parameters);
        output += writeChecksum(outputBase, output, outputLimit, inputBase, inputAddress, inputLimit);

        return (int) (output - outputAddress);
    }

    private static int compressFrame(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, CompressionParameters parameters)
    {
        int blockSize = parameters.getBlockSize();

        int outputSize = (int) (outputLimit - outputAddress);
        int remaining = (int) (inputLimit - inputAddress);

        long output = outputAddress;
        long input = inputAddress;

        CompressionContext context = new CompressionContext(parameters, inputAddress, remaining);
        do {
            checkArgument(outputSize >= SIZE_OF_BLOCK_HEADER + MIN_BLOCK_SIZE, "Output buffer too small");

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

    static int writeCompressedBlock(Object inputBase, long input, int blockSize, Object outputBase, long output, int outputSize, CompressionContext context, boolean lastBlock)
    {
        checkArgument(lastBlock || blockSize == context.parameters.getBlockSize(), "Only last block can be smaller than block size");

        int compressedSize = 0;
        if (blockSize > 0) {
            compressedSize = compressBlock(inputBase, input, blockSize, outputBase, output + SIZE_OF_BLOCK_HEADER, outputSize - SIZE_OF_BLOCK_HEADER, context);
        }

        if (compressedSize == 0) { // block is not compressible
            checkArgument(blockSize + SIZE_OF_BLOCK_HEADER <= outputSize, "Output size too small");

            int blockHeader = (lastBlock ? 1 : 0) | (RAW_BLOCK << 1) | (blockSize << 3);
            put24BitLittleEndian(outputBase, output, blockHeader);
            UNSAFE.copyMemory(inputBase, input, outputBase, output + SIZE_OF_BLOCK_HEADER, blockSize);
            compressedSize = SIZE_OF_BLOCK_HEADER + blockSize;
        }
        else {
            int blockHeader = (lastBlock ? 1 : 0) | (COMPRESSED_BLOCK << 1) | (compressedSize << 3);
            put24BitLittleEndian(outputBase, output, blockHeader);
            compressedSize += SIZE_OF_BLOCK_HEADER;
        }
        return compressedSize;
    }

    private static int compressBlock(Object inputBase, long inputAddress, int inputSize, Object outputBase, long outputAddress, int outputSize, CompressionContext context)
    {
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
            Object outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize)
    {
        // TODO: move this to Strategy
        boolean bypassCompression = (parameters.getStrategy() == CompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);
        if (bypassCompression || literalsSize <= MINIMUM_LITERALS_SIZE) {
            return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }

        int headerSize = 3 + (literalsSize >= 1024 ? 1 : 0) + (literalsSize >= 16384 ? 1 : 0);

        checkArgument(headerSize + 1 <= outputSize, "Output buffer too small");

        int[] counts = new int[MAX_SYMBOL_COUNT]; // TODO: preallocate
        Histogram.count(literals, literalsSize, counts);
        int maxSymbol = Histogram.findMaxSymbol(counts, MAX_SYMBOL);
        int largestCount = Histogram.findLargestCount(counts, maxSymbol);

        long literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        if (largestCount == literalsSize) {
            // all bytes in input are equal
            return rleLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }
        else if (largestCount <= (literalsSize >>> 7) + 4) {
            // heuristic: probably not compressible enough
            return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }

        HuffmanCompressionTable previousTable = context.getPreviousTable();
        HuffmanCompressionTable table;
        int serializedTableSize;
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

            serializedTableSize = newTable.write(outputBase, outputAddress + headerSize, outputSize - headerSize, context.getTableWriterWorkspace());

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
            compressedSize = HuffmanCompressor.compressSingleStream(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
        }
        else {
            compressedSize = HuffmanCompressor.compress4streams(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
        }

        int totalSize = serializedTableSize + compressedSize;
        int minimumGain = calculateMinimumGain(literalsSize, parameters.getStrategy());

        if (compressedSize == 0 || totalSize >= literalsSize - minimumGain) {
            // incompressible or no savings

            // discard any temporary table we might have borrowed above
            context.discardTemporaryTable();

            return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }

        int encodingType = reuseTable ? TREELESS_LITERALS_BLOCK : COMPRESSED_LITERALS_BLOCK;

        // Build header
        switch (headerSize) {
            case 3: { // 2 - 2 - 10 - 10
                int header = encodingType | ((singleStream ? 0 : 1) << 2) | (literalsSize << 4) | (totalSize << 14);
                put24BitLittleEndian(outputBase, outputAddress, header);
                break;
            }
            case 4: { // 2 - 2 - 14 - 14
                int header = encodingType | (2 << 2) | (literalsSize << 4) | (totalSize << 18);
                UNSAFE.putInt(outputBase, outputAddress, header);
                break;
            }
            case 5: { // 2 - 2 - 18 - 18
                int header = encodingType | (3 << 2) | (literalsSize << 4) | (totalSize << 22);
                UNSAFE.putInt(outputBase, outputAddress, header);
                UNSAFE.putByte(outputBase, outputAddress + SIZE_OF_INT, (byte) (totalSize >>> 10));
                break;
            }
            default:  // not possible : headerSize is {3,4,5}
                throw new IllegalStateException();
        }

        return headerSize + totalSize;
    }

    private static int rleLiterals(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize)
    {
        int headerSize = 1 + (inputSize > 31 ? 1 : 0) + (inputSize > 4095 ? 1 : 0);

        switch (headerSize) {
            case 1: // 2 - 1 - 5
                UNSAFE.putByte(outputBase, outputAddress, (byte) (RLE_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2: // 2 - 2 - 12
                UNSAFE.putShort(outputBase, outputAddress, (short) (RLE_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3: // 2 - 2 - 20
                UNSAFE.putInt(outputBase, outputAddress, RLE_LITERALS_BLOCK | 3 << 2 | inputSize << 4);
                break;
            default:   // impossible. headerSize is {1,2,3}
                throw new IllegalStateException();
        }

        UNSAFE.putByte(outputBase, outputAddress + headerSize, UNSAFE.getByte(inputBase, inputAddress));

        return headerSize + 1;
    }

    private static int calculateMinimumGain(int inputSize, CompressionParameters.Strategy strategy)
    {
        // TODO: move this to Strategy to avoid hardcoding a specific strategy here
        int minLog = strategy == CompressionParameters.Strategy.BTULTRA ? 7 : 6;
        return (inputSize >>> minLog) + 2;
    }

    private static int rawLiterals(Object outputBase, long outputAddress, int outputSize, Object inputBase, long inputAddress, int inputSize)
    {
        int headerSize = 1;
        if (inputSize >= 32) {
            headerSize++;
        }
        if (inputSize >= 4096) {
            headerSize++;
        }

        checkArgument(inputSize + headerSize <= outputSize, "Output buffer too small");

        switch (headerSize) {
            case 1:
                UNSAFE.putByte(outputBase, outputAddress, (byte) (RAW_LITERALS_BLOCK | (inputSize << 3)));
                break;
            case 2:
                UNSAFE.putShort(outputBase, outputAddress, (short) (RAW_LITERALS_BLOCK | (1 << 2) | (inputSize << 4)));
                break;
            case 3:
                put24BitLittleEndian(outputBase, outputAddress, RAW_LITERALS_BLOCK | (3 << 2) | (inputSize << 4));
                break;
            default:
                throw new AssertionError();
        }

        // TODO: ensure this test is correct
        checkArgument(inputSize + 1 <= outputSize, "Output buffer too small");

        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress + headerSize, inputSize);

        return headerSize + inputSize;
    }

    /** Decompresses the input data from {@param input} and writes the decompressed data to the {@param output}. */
    public static int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        // Read the uncompressed size from the input buffer
        int uncompressedSize = UNSAFE.getInt(input, inputAddress);
        inputAddress += SIZE_OF_INT;

        System.out.println("Uncompressed Size: " + uncompressedSize);

        return doDecompression(input, inputAddress, inputLimit, output, outputAddress, outputLimit);
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
    private static Object literalsBase;
    private static long literalsAddress;
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
            final Object inputBase,
            final long inputAddress,
            final long inputLimit,
            final Object outputBase,
            final long outputAddress,
            final long outputLimit)
    {
        if (outputAddress == outputLimit) {
            return 0;
        }

        long input = inputAddress;
        long output = outputAddress;

        while (input < inputLimit) {
            reset();
            long outputStart = output;
            input += verifyMagic(inputBase, input, inputLimit);

            FrameHeader frameHeader = readFrameHeader(inputBase, input, inputLimit);
            input += frameHeader.headerSize;

            boolean lastBlock;
            do {
                verify(input + SIZE_OF_BLOCK_HEADER <= inputLimit, input, "Not enough input bytes");

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
                        decodedSize = decodeCompressedBlock(inputBase, input, blockSize, outputBase, output, outputLimit, frameHeader.windowSize, outputAddress);
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
                int checksum = UNSAFE.getInt(inputBase, input);
                if (checksum != (int) hash) {
                    throw new MalformedInputException(input, format("Bad checksum. Expected: %s, actual: %s", Integer.toHexString(checksum), Integer.toHexString((int) hash)));
                }

                input += SIZE_OF_INT;
            }
        }

        return (int) (output - outputAddress);
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

    static int decodeRawBlock(Object inputBase, long inputAddress, int blockSize, Object outputBase, long outputAddress, long outputLimit)
    {
        verify(outputAddress + blockSize <= outputLimit, inputAddress, "Output buffer too small");

        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, outputAddress, blockSize);
        return blockSize;
    }

    static int decodeRleBlock(int size, Object inputBase, long inputAddress, Object outputBase, long outputAddress, long outputLimit)
    {
        verify(outputAddress + size <= outputLimit, inputAddress, "Output buffer too small");

        long output = outputAddress;
        long value = UNSAFE.getByte(inputBase, inputAddress) & 0xFFL;

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
                UNSAFE.putLong(outputBase, output, packed);
                output += SIZE_OF_LONG;
                remaining -= SIZE_OF_LONG;
            }
            while (remaining >= SIZE_OF_LONG);
        }

        for (int i = 0; i < remaining; i++) {
            UNSAFE.putByte(outputBase, output, (byte) value);
            output++;
        }

        return size;
    }

    static int decodeCompressedBlock(
            Object inputBase,
            final long inputAddress,
            int blockSize,
            Object outputBase,
            long outputAddress,
            long outputLimit,
            int windowSize,
            long outputAbsoluteBaseAddress)
    {
        long inputLimit = inputAddress + blockSize;
        long input = inputAddress;

        verify(blockSize <= MAX_BLOCK_SIZE, input, "Expected match length table to be present");
        verify(blockSize >= MIN_BLOCK_SIZE, input, "Compressed block size too small");

        // decode literals
        int literalsBlockType = UNSAFE.getByte(inputBase, input) & 0b11;

        switch (literalsBlockType) {
            case RAW_LITERALS_BLOCK: {
                input += decodeRawLiterals(inputBase, input, inputLimit);
                break;
            }
            case RLE_LITERALS_BLOCK: {
                input += decodeRleLiterals(inputBase, input, blockSize);
                break;
            }
            case TREELESS_LITERALS_BLOCK:
                verify(huffman.isLoaded(), input, "Dictionary is corrupted");
            case COMPRESSED_LITERALS_BLOCK: {
                input += decodeCompressedLiterals(inputBase, input, blockSize, literalsBlockType);
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
                outputAbsoluteBaseAddress);
    }

    private static int decompressSequences(
            final Object inputBase, final long inputAddress, final long inputLimit,
            final Object outputBase, final long outputAddress, final long outputLimit,
            final Object literalsBase, final long literalsAddress, final long literalsLimit,
            long outputAbsoluteBaseAddress)
    {
        final long fastOutputLimit = outputLimit - SIZE_OF_LONG;
        final long fastMatchOutputLimit = fastOutputLimit - SIZE_OF_LONG;

        long input = inputAddress;
        long output = outputAddress;

        long literalsInput = literalsAddress;

        int size = (int) (inputLimit - inputAddress);
        verify(size >= MIN_SEQUENCES_SIZE, input, "Not enough input bytes");

        // decode header
        int sequenceCount = UNSAFE.getByte(inputBase, input++) & 0xFF;
        if (sequenceCount != 0) {
            if (sequenceCount == 255) {
                verify(input + SIZE_OF_SHORT <= inputLimit, input, "Not enough input bytes");
                sequenceCount = (UNSAFE.getShort(inputBase, input) & 0xFFFF) + LONG_NUMBER_OF_SEQUENCES;
                input += SIZE_OF_SHORT;
            }
            else if (sequenceCount > 127) {
                verify(input < inputLimit, input, "Not enough input bytes");
                sequenceCount = ((sequenceCount - 128) << 8) + (UNSAFE.getByte(inputBase, input++) & 0xFF);
            }

            verify(input + SIZE_OF_INT <= inputLimit, input, "Not enough input bytes");

            byte type = UNSAFE.getByte(inputBase, input++);

            int literalsLengthType = (type & 0xFF) >>> 6;
            int offsetCodesType = (type >>> 4) & 0b11;
            int matchLengthType = (type >>> 2) & 0b11;

            input = computeLiteralsTable(literalsLengthType, inputBase, input, inputLimit);
            input = computeOffsetsTable(offsetCodesType, inputBase, input, inputLimit);
            input = computeMatchLengthTable(matchLengthType, inputBase, input, inputLimit);

            // decompress sequences
            BitInputStream.Initializer initializer = new BitInputStream.Initializer(inputBase, input, inputLimit);
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

                BitInputStream.Loader loader = new BitInputStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
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
                    offset += peekBits(bitsConsumed, bits, offsetBits);
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
                    matchLength += peekBits(bitsConsumed, bits, matchLengthBits);
                    bitsConsumed += matchLengthBits;
                }

                int literalsLength = LITERALS_LENGTH_BASE[literalsLengthCode];
                if (literalsLengthCode > 15) {
                    literalsLength += peekBits(bitsConsumed, bits, literalsLengthBits);
                    bitsConsumed += literalsLengthBits;
                }

                int totalBits = literalsLengthBits + matchLengthBits + offsetBits;
                if (totalBits > 64 - 7 - (LITERAL_LENGTH_TABLE_LOG + MATCH_LENGTH_TABLE_LOG + OFFSET_TABLE_LOG)) {
                    BitInputStream.Loader loader1 = new BitInputStream.Loader(inputBase, input, currentAddress, bits, bitsConsumed);
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
                    executeLastSequence(outputBase, output, literalOutputLimit, matchOutputLimit, fastOutputLimit, literalsInput, matchAddress);
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

    private static long copyLastLiteral(long input, Object literalsBase, long literalsInput, long literalsLimit, Object outputBase, long output, long outputLimit)
    {
        long lastLiteralsSize = literalsLimit - literalsInput;
        verify(output + lastLiteralsSize <= outputLimit, input, "Output buffer too small");
        UNSAFE.copyMemory(literalsBase, literalsInput, outputBase, output, lastLiteralsSize);
        output += lastLiteralsSize;
        return output;
    }

    private static void copyMatch(Object outputBase,
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

    private static void copyMatchTail(Object outputBase, long fastOutputLimit, long output, long matchOutputLimit, long matchAddress, int matchLength, long fastMatchOutputLimit)
    {
        // fastMatchOutputLimit is just fastOutputLimit - SIZE_OF_LONG. It needs to be passed in so that it can be computed once for the
        // whole invocation to decompressSequences. Otherwise, we'd just compute it here.
        // If matchOutputLimit is < fastMatchOutputLimit, we know that even after the head (8 bytes) has been copied, the output pointer
        // will be within fastOutputLimit, so it's safe to copy blindly before checking the limit condition
        if (matchOutputLimit < fastMatchOutputLimit) {
            int copied = 0;
            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                output += SIZE_OF_LONG;
                matchAddress += SIZE_OF_LONG;
                copied += SIZE_OF_LONG;
            }
            while (copied < matchLength);
        }
        else {
            while (output < fastOutputLimit) {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            while (output < matchOutputLimit) {
                UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
            }
        }
    }

    private static long copyMatchHead(Object outputBase, long output, int offset, long matchAddress)
    {
        // copy match
        if (offset < 8) {
            // 8 bytes apart so that we can copy long-at-a-time below
            int increment32 = DEC_32_TABLE[offset];
            int decrement64 = DEC_64_TABLE[offset];

            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
            UNSAFE.putByte(outputBase, output + 1, UNSAFE.getByte(outputBase, matchAddress + 1));
            UNSAFE.putByte(outputBase, output + 2, UNSAFE.getByte(outputBase, matchAddress + 2));
            UNSAFE.putByte(outputBase, output + 3, UNSAFE.getByte(outputBase, matchAddress + 3));
            matchAddress += increment32;

            UNSAFE.putInt(outputBase, output + 4, UNSAFE.getInt(outputBase, matchAddress));
            matchAddress -= decrement64;
        }
        else {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
            matchAddress += SIZE_OF_LONG;
        }
        return matchAddress;
    }

    private static long copyLiterals(Object outputBase, Object literalsBase, long output, long literalsInput, long literalOutputLimit)
    {
        long literalInput = literalsInput;
        do {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(literalsBase, literalInput));
            output += SIZE_OF_LONG;
            literalInput += SIZE_OF_LONG;
        }
        while (output < literalOutputLimit);
        output = literalOutputLimit; // correction in case we over-copied
        return output;
    }

    private static long computeMatchLengthTable(int matchLengthType, Object inputBase, long input, long inputLimit)
    {
        switch (matchLengthType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
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
                input += fse.readFseTable(matchLengthTable, inputBase, input, inputLimit, MAX_MATCH_LENGTH_SYMBOL, MATCH_LENGTH_TABLE_LOG);
                currentMatchLengthTable = matchLengthTable;
                break;
            default:
                throw fail(input, "Invalid match length encoding type");
        }
        return input;
    }

    private static long computeOffsetsTable(int offsetCodesType, Object inputBase, long input, long inputLimit)
    {
        switch (offsetCodesType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
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
                input += fse.readFseTable(offsetCodesTable, inputBase, input, inputLimit, DEFAULT_MAX_OFFSET_CODE_SYMBOL, OFFSET_TABLE_LOG);
                currentOffsetCodesTable = offsetCodesTable;
                break;
            default:
                throw fail(input, "Invalid offset code encoding type");
        }
        return input;
    }

    private static long computeLiteralsTable(int literalsLengthType, Object inputBase, long input, long inputLimit)
    {
        switch (literalsLengthType) {
            case SEQUENCE_ENCODING_RLE:
                verify(input < inputLimit, input, "Not enough input bytes");

                byte value = UNSAFE.getByte(inputBase, input++);
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
                input += fse.readFseTable(literalsLengthTable, inputBase, input, inputLimit, MAX_LITERALS_LENGTH_SYMBOL, LITERAL_LENGTH_TABLE_LOG);
                currentLiteralsLengthTable = literalsLengthTable;
                break;
            default:
                throw fail(input, "Invalid literals length encoding type");
        }
        return input;
    }

    private static void executeLastSequence(Object outputBase, long output, long literalOutputLimit, long matchOutputLimit, long fastOutputLimit, long literalInput, long matchAddress)
    {
        // copy literals
        if (output < fastOutputLimit) {
            // wild copy
            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(literalsBase, literalInput));
                output += SIZE_OF_LONG;
                literalInput += SIZE_OF_LONG;
            }
            while (output < fastOutputLimit);

            literalInput -= output - fastOutputLimit;
            output = fastOutputLimit;
        }

        while (output < literalOutputLimit) {
            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(literalsBase, literalInput));
            output++;
            literalInput++;
        }

        // copy match
        while (output < matchOutputLimit) {
            UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
            output++;
            matchAddress++;
        }
    }

    private static int decodeCompressedLiterals(Object inputBase, final long inputAddress, int blockSize, int literalsBlockType)
    {
        long input = inputAddress;
        verify(blockSize >= 5, input, "Not enough input bytes");

        // compressed
        int compressedSize;
        int uncompressedSize;
        boolean singleStream = false;
        int headerSize;
        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;
        switch (type) {
            case 0:
                singleStream = true;
            case 1: {
                int header = UNSAFE.getInt(inputBase, input);

                headerSize = 3;
                uncompressedSize = (header >>> 4) & mask(10);
                compressedSize = (header >>> 14) & mask(10);
                break;
            }
            case 2: {
                int header = UNSAFE.getInt(inputBase, input);

                headerSize = 4;
                uncompressedSize = (header >>> 4) & mask(14);
                compressedSize = (header >>> 18) & mask(14);
                break;
            }
            case 3: {
                // read 5 little-endian bytes
                long header = UNSAFE.getByte(inputBase, input) & 0xFF |
                        (UNSAFE.getInt(inputBase, input + 1) & 0xFFFF_FFFFL) << 8;

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
            input += huffman.readTable(inputBase, input, compressedSize);
        }

        literalsBase = literals;
        literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        literalsLimit = ARRAY_BYTE_BASE_OFFSET + uncompressedSize;

        if (singleStream) {
            huffman.decodeSingleStream(inputBase, input, inputLimit, literals, literalsAddress, literalsLimit);
        }
        else {
            huffman.decode4Streams(inputBase, input, inputLimit, literals, literalsAddress, literalsLimit);
        }

        return headerSize + compressedSize;
    }

    private static int decodeRleLiterals(Object inputBase, final long inputAddress, int blockSize)
    {
        long input = inputAddress;
        int outputSize;

        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;
        switch (type) {
            case 0:
            case 2:
                outputSize = (UNSAFE.getByte(inputBase, input) & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                outputSize = (UNSAFE.getShort(inputBase, input) & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // we need at least 4 bytes (3 for the header, 1 for the payload)
                verify(blockSize >= SIZE_OF_INT, input, "Not enough input bytes");
                outputSize = (UNSAFE.getInt(inputBase, input) & 0xFF_FFFF) >>> 4;
                input += 3;
                break;
            default:
                throw fail(input, "Invalid RLE literals header encoding type");
        }

        verify(outputSize <= MAX_BLOCK_SIZE, input, "Output exceeds maximum block size");

        byte value = UNSAFE.getByte(inputBase, input++);
        Arrays.fill(literals, 0, outputSize + SIZE_OF_LONG, value);

        literalsBase = literals;
        literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        literalsLimit = ARRAY_BYTE_BASE_OFFSET + outputSize;

        return (int) (input - inputAddress);
    }

    private static int decodeRawLiterals(Object inputBase, final long inputAddress, long inputLimit)
    {
        long input = inputAddress;
        int type = (UNSAFE.getByte(inputBase, input) >> 2) & 0b11;

        int literalSize;
        switch (type) {
            case 0:
            case 2:
                literalSize = (UNSAFE.getByte(inputBase, input) & 0xFF) >>> 3;
                input++;
                break;
            case 1:
                literalSize = (UNSAFE.getShort(inputBase, input) & 0xFFFF) >>> 4;
                input += 2;
                break;
            case 3:
                // read 3 little-endian bytes
                int header = ((UNSAFE.getByte(inputBase, input) & 0xFF) |
                        ((UNSAFE.getShort(inputBase, input + 1) & 0xFFFF) << 8));

                literalSize = header >>> 4;
                input += 3;
                break;
            default:
                throw fail(input, "Invalid raw literals header encoding type");
        }

        verify(input + literalSize <= inputLimit, input, "Not enough input bytes");

        // Set literals pointer to [input, literalSize], but only if we can copy 8 bytes at a time during sequence decoding
        // Otherwise, copy literals into buffer that's big enough to guarantee that
        if (literalSize > (inputLimit - input) - SIZE_OF_LONG) {
            literalsBase = literals;
            literalsAddress = ARRAY_BYTE_BASE_OFFSET;
            literalsLimit = ARRAY_BYTE_BASE_OFFSET + literalSize;

            UNSAFE.copyMemory(inputBase, input, literals, literalsAddress, literalSize);
            Arrays.fill(literals, literalSize, literalSize + SIZE_OF_LONG, (byte) 0);
        }
        else {
            literalsBase = inputBase;
            literalsAddress = input;
            literalsLimit = literalsAddress + literalSize;
        }
        input += literalSize;

        return (int) (input - inputAddress);
    }

    static FrameHeader readFrameHeader(final Object inputBase, final long inputAddress, final long inputLimit)
    {
        long input = inputAddress;
        verify(input < inputLimit, input, "Not enough input bytes");

        int frameHeaderDescriptor = UNSAFE.getByte(inputBase, input++) & 0xFF;
        boolean singleSegment = (frameHeaderDescriptor & 0b100000) != 0;
        int dictionaryDescriptor = frameHeaderDescriptor & 0b11;
        int contentSizeDescriptor = frameHeaderDescriptor >>> 6;

        int headerSize = 1 +
                (singleSegment ? 0 : 1) +
                (dictionaryDescriptor == 0 ? 0 : (1 << (dictionaryDescriptor - 1))) +
                (contentSizeDescriptor == 0 ? (singleSegment ? 1 : 0) : (1 << contentSizeDescriptor));

        verify(headerSize <= inputLimit - inputAddress, input, "Not enough input bytes");

        // decode window size
        int windowSize = -1;
        if (!singleSegment) {
            int windowDescriptor = UNSAFE.getByte(inputBase, input++) & 0xFF;
            int exponent = windowDescriptor >>> 3;
            int mantissa = windowDescriptor & 0b111;

            int base = 1 << (MIN_WINDOW_LOG + exponent);
            windowSize = base + (base / 8) * mantissa;
        }

        // decode dictionary id
        long dictionaryId = -1;
        switch (dictionaryDescriptor) {
            case 1:
                dictionaryId = UNSAFE.getByte(inputBase, input) & 0xFF;
                input += SIZE_OF_BYTE;
                break;
            case 2:
                dictionaryId = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                input += SIZE_OF_SHORT;
                break;
            case 3:
                dictionaryId = UNSAFE.getInt(inputBase, input) & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
        }
        verify(dictionaryId == -1, input, "Custom dictionaries not supported");

        // decode content size
        long contentSize = -1;
        switch (contentSizeDescriptor) {
            case 0:
                if (singleSegment) {
                    contentSize = UNSAFE.getByte(inputBase, input) & 0xFF;
                    input += SIZE_OF_BYTE;
                }
                break;
            case 1:
                contentSize = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                contentSize += 256;
                input += SIZE_OF_SHORT;
                break;
            case 2:
                contentSize = UNSAFE.getInt(inputBase, input) & 0xFFFF_FFFFL;
                input += SIZE_OF_INT;
                break;
            case 3:
                contentSize = UNSAFE.getLong(inputBase, input);
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

    public static long returnDecompressedSize(final Object inputBase, final long inputAddress, final long inputLimit)
    {
        long input = inputAddress;
        input += verifyMagic(inputBase, input, inputLimit);
        return readFrameHeader(inputBase, input, inputLimit).contentSize;
    }

    static int verifyMagic(Object inputBase, long inputAddress, long inputLimit)
    {
        verify(inputLimit - inputAddress >= 4, inputAddress, "Not enough input bytes");

        int magic = UNSAFE.getInt(inputBase, inputAddress);
        if (magic != MAGIC_NUMBER) {
            if (magic == V07_MAGIC_NUMBER) {
                throw new MalformedInputException(inputAddress, "Data encoded in unsupported ZSTD v0.7 format");
            }
            throw new MalformedInputException(inputAddress, "Invalid magic prefix: " + Integer.toHexString(magic));
        }

        return SIZE_OF_INT;
    }
}
