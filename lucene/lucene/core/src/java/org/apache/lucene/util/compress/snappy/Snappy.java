/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.snappy;

import org.apache.lucene.util.MalformedInputException;

import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class Snappy {

    private final short[] table = new short[SnappyRawCompressor.MAX_HASH_TABLE_SIZE];

    public int maxCompressedLength(int uncompressedSize)
    {
        return SnappyRawCompressor.maxCompressedLength(uncompressedSize);
    }

    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        return SnappyRawCompressor.compress(input, inputAddress, inputLimit, output, outputAddress, outputLimit, table);
    }

    public int getRetainedSizeInBytes(int inputLength)
    {
        return SnappyRawCompressor.getHashTableSize(inputLength);
    }

    public int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        long compressedAddress = ARRAY_BYTE_BASE_OFFSET + compressedOffset;
        long compressedLimit = ARRAY_BYTE_BASE_OFFSET + compressed.length;

        return SnappyRawDecompressor.getUncompressedLength(compressed, compressedAddress, compressedLimit);
    }

    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        return SnappyRawDecompressor.decompress(input, inputAddress, inputLimit, output, outputAddress, outputLimit);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
