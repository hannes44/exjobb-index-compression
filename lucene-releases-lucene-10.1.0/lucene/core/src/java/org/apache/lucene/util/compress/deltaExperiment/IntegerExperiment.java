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
import java.util.Arrays;

public final class IntegerExperiment {

    private IntegerExperiment() {}

    private static int maxIntegerArraySize(int length) {
        return Math.ceilDiv(length, Integer.BYTES);
    }

    private static void insertIntegersFromByteArray(int[] integerArray, byte[] bytes, int bytesToTransfer) {
        int intIndex = 0;
        int i = 0;

        //System.out.println("Bytes Array : " + Arrays.toString(bytes));
        // Process 4 bytes at a time (most performant)
        while (i + 3 < bytesToTransfer) {
            integerArray[intIndex++] =
                    ((bytes[i++] & 0xFF) << 24) |
                    ((bytes[i++] & 0xFF) << 16) |
                    ((bytes[i++] & 0xFF) << 8) |
                    (bytes[i++] & 0xFF);
        }

        // Handle remaining bytes (if any)
        if (i < bytesToTransfer) {
            int remainingInt = 0;
            int shift = 24;
            while (i < bytesToTransfer) {
                remainingInt |= (bytes[i++] & 0xFF) << shift;
                shift -= 8;
            }
            integerArray[intIndex] = remainingInt;
        }
    }

    public static void deltaEncodeIntegers(int[] values) {
        for (int i = values.length - 1; i > 0; i--) {
            values[i] -= values[i-1];
        }
    }

    public static void compress(byte[] in, int originalLength, DataOutput out) throws IOException {
        // Create Int Array large enough to fit all bytes
        int[] integerArray = new int[maxIntegerArraySize(originalLength)];
        // Insert all bytes into Int Array
        insertIntegersFromByteArray(integerArray, in, originalLength);
        // Now delta encode the integers
        deltaEncodeIntegers(integerArray);
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

    public static void deltaDecodeIntegers(int[] values) {
        for (int i = 1; i < values.length; i++) {
            values[i] += values[i-1];
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
        // Decode delta offsets
        deltaDecodeIntegers(integerArray);
        // Extract original bytes from Ints
        extractBytesFromIntegerArray(integerArray, out, originalByteLength);
    }
}
