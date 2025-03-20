/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_LONG;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_SHORT;

public final class HuffmanCompressor
{
    private HuffmanCompressor()
    {
    }

    public static int compress4streams(DataOutput outputBase, long outputAddress, int outputSize, byte[] inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table) throws IOException {
        long input = inputAddress;
        long inputLimit = inputAddress + inputSize;
        long output = outputAddress;
        long outputLimit = outputAddress + outputSize;

        int segmentSize = (inputSize + 3) / 4;

        if (outputSize < 6 /* jump table */ + 1 /* first stream */ + 1 /* second stream */ + 1 /* third stream */ + 8 /* 8 bytes minimum needed by the bitstream encoder */) {
            return 0; // minimum space to compress successfully
        }

        if (inputSize <= 6 + 1 + 1 + 1) { // jump table + one byte per stream
            return 0;  // no saving possible: input too small
        }

        output += SIZE_OF_SHORT + SIZE_OF_SHORT + SIZE_OF_SHORT; // jump table

        int compressedSize;
        ByteBuffersDataOutput outputBuffer = new ByteBuffersDataOutput();

        // first segment
        compressedSize = compressSingleStream(outputBuffer, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.writeShort((short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // second segment
        compressedSize = compressSingleStream(outputBuffer, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.writeShort((short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // third segment
        compressedSize = compressSingleStream(outputBuffer, output, (int) (outputLimit - output), inputBase, input, segmentSize, table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBase.writeShort((short) compressedSize);
        output += compressedSize;
        input += segmentSize;

        // fourth segment
        compressedSize = compressSingleStream(outputBuffer, output, (int) (outputLimit - output), inputBase, input, (int) (inputLimit - input), table);
        if (compressedSize == 0) {
            return 0;
        }
        outputBuffer.copyTo(outputBase);
        output += compressedSize;

        return (int) (output - outputAddress);
    }

    @SuppressWarnings("fallthrough")
    public static int compressSingleStream(DataOutput outputBase, long outputAddress, int outputSize, byte[] inputBase, long inputAddress, int inputSize, HuffmanCompressionTable table) throws IOException {
        if (outputSize < SIZE_OF_LONG) {
            return 0;
        }

        BitOutputStream bitstream = new BitOutputStream(outputBase, outputAddress, outputSize);
        int input = (int) inputAddress;

        int n = inputSize & ~3; // join to mod 4

        switch (inputSize & 3) {
            case 3:
                table.encodeSymbol(bitstream, inputBase[input + n + 2] & 0xFF);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 2:
                table.encodeSymbol(bitstream, inputBase[input + n + 1] & 0xFF);
                if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                    bitstream.flush();
                }
                // fall-through
            case 1:
                table.encodeSymbol(bitstream, inputBase[input + n + 0] & 0xFF);
                bitstream.flush();
                // fall-through
            case 0: /* fall-through */
            default:
                break;
        }

        for (; n > 0; n -= 4) {  // note: n & 3 == 0 at this stage
            table.encodeSymbol(bitstream, inputBase[input + n - 1] & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase[input + n - 2] & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 4 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase[input + n - 3] & 0xFF);
            if (SIZE_OF_LONG * 8 < Huffman.MAX_TABLE_LOG * 2 + 7) {
                bitstream.flush();
            }
            table.encodeSymbol(bitstream, inputBase[input + n - 4] & 0xFF);
            bitstream.flush();
        }

        return bitstream.close();
    }
}
