/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

import static java.util.Objects.requireNonNull;
import static java.lang.String.format;

import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.*;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeHuffman.MAX_SYMBOL;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeHuffman.MAX_SYMBOL_COUNT;
import static org.apache.lucene.util.UnsafeUtil.UNSAFE;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDDecompressor.readFrameHeader;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDDecompressor.verifyMagic;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.checkArgument;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.put24BitLittleEndian;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
/**
 * Zstandard compression routine.
 */
public final class UnsafeZSTDCompressor {

    private UnsafeZSTDCompressor() {}

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

        long hash = UnsafeXxHash64.hash(0, inputBase, inputAddress, inputSize);

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
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset + SIZE_OF_INT; // Reserve space for the compressed size

        int compressedLength = doCompression(input, inputAddress, inputAddress + inputLength, output, outputAddress, outputAddress + maxOutputLength, UnsafeCompressionParameters.DEFAULT_COMPRESSION_LEVEL);

        // Write the final compressed size to the output buffer; For use in decompression
        outputAddress -= SIZE_OF_INT;
        UNSAFE.putInt(output, outputAddress, compressedLength);

        return SIZE_OF_INT + compressedLength;
    }

    public static int doCompression(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, int compressionLevel)
    {
        int inputSize = (int) (inputLimit - inputAddress);

        UnsafeCompressionParameters parameters = UnsafeCompressionParameters.compute(compressionLevel, inputSize);

        long output = outputAddress;

        output += writeMagic(outputBase, output, outputLimit);
        output += writeFrameHeader(outputBase, output, outputLimit, inputSize, parameters.getWindowSize());
        output += compressFrame(inputBase, inputAddress, inputLimit, outputBase, output, outputLimit, parameters);
        output += writeChecksum(outputBase, output, outputLimit, inputBase, inputAddress, inputLimit);

        return (int) (output - outputAddress);
    }

    private static int compressFrame(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit, UnsafeCompressionParameters parameters)
    {
        int blockSize = parameters.getBlockSize();

        int outputSize = (int) (outputLimit - outputAddress);
        int remaining = (int) (inputLimit - inputAddress);

        long output = outputAddress;
        long input = inputAddress;

        UnsafeCompressionContext context = new UnsafeCompressionContext(parameters, inputAddress, remaining);
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

    static int writeCompressedBlock(Object inputBase, long input, int blockSize, Object outputBase, long output, int outputSize, UnsafeCompressionContext context, boolean lastBlock)
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

    private static int compressBlock(Object inputBase, long inputAddress, int inputSize, Object outputBase, long outputAddress, int outputSize, UnsafeCompressionContext context)
    {
        if (inputSize < MIN_BLOCK_SIZE + SIZE_OF_BLOCK_HEADER + 1) {
            //  don't even attempt compression below a certain input size
            return 0;
        }

        UnsafeCompressionParameters parameters = context.parameters;
        context.unsafeBlockCompressionState.enforceMaxDistance(inputAddress + inputSize, parameters.getWindowSize());
        context.unsafeSequenceStore.reset();

        int lastLiteralsSize = parameters.getStrategy()
                .getCompressor()
                .compressBlock(inputBase, inputAddress, inputSize, context.unsafeSequenceStore, context.unsafeBlockCompressionState, context.offsets, parameters);

        long lastLiteralsAddress = inputAddress + inputSize - lastLiteralsSize;

        // append [lastLiteralsAddress .. lastLiteralsSize] to sequenceStore literals buffer
        context.unsafeSequenceStore.appendLiterals(inputBase, lastLiteralsAddress, lastLiteralsSize);

        // convert length/offsets into codes
        context.unsafeSequenceStore.generateCodes();

        long outputLimit = outputAddress + outputSize;
        long output = outputAddress;

        int compressedLiteralsSize = encodeLiterals(
                context.huffmanContext,
                parameters,
                outputBase,
                output,
                (int) (outputLimit - output),
                context.unsafeSequenceStore.literalsBuffer,
                context.unsafeSequenceStore.literalsLength);
        output += compressedLiteralsSize;

        int compressedSequencesSize = UnsafeSequenceEncoder.compressSequences(outputBase, output, (int) (outputLimit - output), context.unsafeSequenceStore, parameters.getStrategy(), context.unsafeSequenceEncodingContext);

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
            UnsafeHuffmanCompressionContext context,
            UnsafeCompressionParameters parameters,
            Object outputBase,
            long outputAddress,
            int outputSize,
            byte[] literals,
            int literalsSize)
    {
        // TODO: move this to Strategy
        boolean bypassCompression = (parameters.getStrategy() == UnsafeCompressionParameters.Strategy.FAST) && (parameters.getTargetLength() > 0);
        if (bypassCompression || literalsSize <= MINIMUM_LITERALS_SIZE) {
            return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }

        int headerSize = 3 + (literalsSize >= 1024 ? 1 : 0) + (literalsSize >= 16384 ? 1 : 0);

        checkArgument(headerSize + 1 <= outputSize, "Output buffer too small");

        int[] counts = new int[MAX_SYMBOL_COUNT]; // TODO: preallocate
        UnsafeHistogram.count(literals, literalsSize, counts);
        int maxSymbol = UnsafeHistogram.findMaxSymbol(counts, MAX_SYMBOL);
        int largestCount = UnsafeHistogram.findLargestCount(counts, maxSymbol);

        long literalsAddress = ARRAY_BYTE_BASE_OFFSET;
        if (largestCount == literalsSize) {
            // all bytes in input are equal
            return rleLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }
        else if (largestCount <= (literalsSize >>> 7) + 4) {
            // heuristic: probably not compressible enough
            return rawLiterals(outputBase, outputAddress, outputSize, literals, ARRAY_BYTE_BASE_OFFSET, literalsSize);
        }

        UnsafeHuffmanCompressionTable previousTable = context.getPreviousTable();
        UnsafeHuffmanCompressionTable table;
        int serializedTableSize;
        boolean reuseTable;

        boolean canReuse = previousTable.isValid(counts, maxSymbol);

        // heuristic: use existing table for small inputs if valid
        // TODO: move to Strategy
        boolean preferReuse = parameters.getStrategy().ordinal() < UnsafeCompressionParameters.Strategy.LAZY.ordinal() && literalsSize <= 1024;
        if (preferReuse && canReuse) {
            table = previousTable;
            reuseTable = true;
            serializedTableSize = 0;
        }
        else {
            UnsafeHuffmanCompressionTable newTable = context.borrowTemporaryTable();

            newTable.initialize(
                    counts,
                    maxSymbol,
                    UnsafeHuffmanCompressionTable.optimalNumberOfBits(MAX_HUFFMAN_TABLE_LOG, literalsSize, maxSymbol),
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
            compressedSize = UnsafeHuffmanCompressor.compressSingleStream(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
        }
        else {
            compressedSize = UnsafeHuffmanCompressor.compress4streams(outputBase, outputAddress + headerSize + serializedTableSize, outputSize - headerSize - serializedTableSize, literals, literalsAddress, literalsSize, table);
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

    private static int calculateMinimumGain(int inputSize, UnsafeCompressionParameters.Strategy strategy)
    {
        // TODO: move this to Strategy to avoid hardcoding a specific strategy here
        int minLog = strategy == UnsafeCompressionParameters.Strategy.BTULTRA ? 7 : 6;
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

    public static long returnDecompressedSize(final Object inputBase, final long inputAddress, final long inputLimit)
    {
        long input = inputAddress;
        input += verifyMagic(inputBase, input, inputLimit);
        return readFrameHeader(inputBase, input, inputLimit).contentSize;
    }
}
