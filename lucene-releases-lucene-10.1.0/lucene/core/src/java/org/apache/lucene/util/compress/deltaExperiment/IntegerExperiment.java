/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.deltaExperiment;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

public final class IntegerExperiment {

    private IntegerExperiment() {}

    private static int maxIntegerArraySize(int length) {
        return (length / Integer.BYTES) + (length % Integer.BYTES);
    }

    private static void insertIntegersFromByteArray(int[] integerArray, byte[] bytes) {
        int intIndex = 0;
        int i = 0;

        // Process 4 bytes at a time (most performant)
        while (i + 3 < bytes.length) {
            integerArray[intIndex++] =
                    ((bytes[i++] & 0xFF) << 24) |
                    ((bytes[i++] & 0xFF) << 16) |
                    ((bytes[i++] & 0xFF) << 8) |
                    (bytes[i++] & 0xFF);
        }

        // Handle remaining bytes (if any)
        if (i < bytes.length) {
            int remainingInt = 0;
            int shift = 24;
            while (i < bytes.length) {
                remainingInt |= (bytes[i++] & 0xFF) << shift;
                shift -= 8;
            }
            integerArray[intIndex] = remainingInt;
        }
    }

    public static void compress(byte[] in, int originalLength, DataOutput out) throws IOException {
        // Create Int Array large enough to fit all bytes
        int[] integerArray = new int[maxIntegerArraySize(originalLength)];
        // Insert all bytes into Int Array
        insertIntegersFromByteArray(integerArray, in);
        // First we write the number of Ints so that the decompressor knows how many to read
        out.writeVInt(integerArray.length);
        // Write Ints as VInts
        for (int i : integerArray) {
            out.writeVInt(i);
        }
    }

    private static void extractBytesFromIntegerArray(int[] integerArray, byte[] bytes, int originalByteLength) {
        int byteIndex = 0;

        for (int i = 0; i < integerArray.length && byteIndex < bytes.length; i++) {
            int currentInt = integerArray[i];
            // Extract all 4 bytes (or fewer if near the end)
            bytes[byteIndex++] = (byte) ((currentInt >> 24) & 0xFF);
            if (byteIndex < originalByteLength) bytes[byteIndex++] = (byte) ((currentInt >> 16) & 0xFF);
            if (byteIndex < originalByteLength) bytes[byteIndex++] = (byte) ((currentInt >> 8) & 0xFF);
            if (byteIndex < originalByteLength) bytes[byteIndex++] = (byte) (currentInt & 0xFF);
        }
    }

    public static void decompress(DataInput in, byte[] out, int originalByteLength) throws IOException {
        // Read original number of bytes
        int numberOfInts = in.readVInt();
        int[] integerArray = new int[numberOfInts];
        // Read all Ints
        for (int i = 0; i < numberOfInts; i++) {
            integerArray[i] = in.readVInt();
        }
        // Extract original bytes from Ints
        extractBytesFromIntegerArray(integerArray, out, originalByteLength);
    }
}
