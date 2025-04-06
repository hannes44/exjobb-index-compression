/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeSnappy;

import org.apache.lucene.util.MalformedInputException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.lucene.util.UnsafeUtil.UNSAFE;
import static org.apache.lucene.util.compress.unsafeSnappy.UnsafeSnappyConstants.MAX_SNAPPY_HASH_TABLE_SIZE;
import static org.apache.lucene.util.compress.unsafeSnappy.UnsafeSnappyConstants.SIZE_OF_INT;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class UnsafeSnappy {

    public static int maxCompressedLength(int uncompressedSize)
    {
        return UnsafeSnappyRawCompressor.maxCompressedLength(uncompressedSize);
    }

    public static int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength, short[] table)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset + Integer.BYTES;
        long outputLimit = outputAddress + maxOutputLength;

        int compressedLength = UnsafeSnappyRawCompressor.compress(input, inputAddress, inputLimit, output, outputAddress, outputLimit, table);

        outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        UNSAFE.putInt(output, outputAddress, compressedLength);

        return compressedLength + Integer.BYTES;
    }

    public int getRetainedSizeInBytes(int inputLength)
    {
        return UnsafeSnappyRawCompressor.getHashTableSize(inputLength);
    }

    public int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        long compressedAddress = ARRAY_BYTE_BASE_OFFSET + compressedOffset;
        long compressedLimit = ARRAY_BYTE_BASE_OFFSET + compressed.length;

        return UnsafeSnappyRawDecompressor.getUncompressedLength(compressed, compressedAddress, compressedLimit);
    }

    public static int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength, boolean lengthKnown)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        if (!lengthKnown) {
            int compressedSize = UNSAFE.getInt(input, inputAddress);
            inputAddress += SIZE_OF_INT;
            // Update the input limit to the compressed size
            inputLimit = compressedSize + inputAddress;
        }

        return UnsafeSnappyRawDecompressor.decompress(input, inputAddress, inputLimit, output, outputAddress, outputLimit);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
