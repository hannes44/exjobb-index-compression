/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.snappy;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.MalformedInputException;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Compression and decompression routines for Snappy compression format.
 */

public final class Snappy {

    private Snappy() {}

    public static final int MAX_HASH_TABLE_BITS = 14;
    public static final int MAX_HASH_TABLE_SIZE = 1 << MAX_HASH_TABLE_BITS;

    public static short readShort(byte[] data, long address) {
        return (short) BitUtil.VH_NATIVE_SHORT.get(data, (int) address);
    }

    public static int readInt(byte[] data, long address) {
        if (address + Integer.BYTES > data.length) {
//            try {
//                Thread.sleep(1);        // TODO: WTF IS THIS?????? (Does not work without :despair:)
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            return 0;
        }
        return (int) BitUtil.VH_NATIVE_INT.get(data, (int) address);
    }

    public static long readLong(byte[] data, long address) {
        return (long) BitUtil.VH_NATIVE_LONG.get(data, (int) address);
    }

    public static void writeByte(byte[] data, long address, byte value) {
        data[(int) address] = value;
    }

    public static void writeShort(byte[] data, long address, short value) {
        BitUtil.VH_NATIVE_SHORT.set(data, (int) address, value);
    }

    public static void writeLong(byte[] data, long address, long value) {
        BitUtil.VH_NATIVE_LONG.set(data, (int) address, value);
    }

    public static void writeInt(byte[] data, long address, int value) {
        BitUtil.VH_NATIVE_INT.set(data, (int) address, value);
    }

    public static int maxCompressedLength(int uncompressedSize)
    {
        return SnappyRawCompressor.maxCompressedLength(uncompressedSize);
    }

    public static int compress(byte[] input, int inputLength, DataOutput output, short[] table) throws IOException {
        byte[] tempOutput = new byte[SnappyRawCompressor.maxCompressedLength(inputLength)];

        int compressedDataSize = SnappyRawCompressor.compress(input, inputLength, tempOutput, table);

        output.writeInt(compressedDataSize); // VInt?
        output.writeBytes(tempOutput, compressedDataSize);

        return compressedDataSize + Integer.BYTES;
    }

    public int getRetainedSizeInBytes(int inputLength)
    {
        return SnappyRawCompressor.getHashTableSize(inputLength);
    }

    public static void decompress(DataInput input, byte[] output, int maxOutputLength)
            throws MalformedInputException, IOException {
        // Read the compressed data size
        int compressedDataSize = input.readInt(); // VInt?
        byte[] tempInput = new byte[compressedDataSize];
        input.readBytes(tempInput, 0, compressedDataSize);
        SnappyRawDecompressor.decompress(tempInput, 0, compressedDataSize, output, 0, maxOutputLength);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
