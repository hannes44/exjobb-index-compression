/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

import java.util.Arrays;

import static org.apache.lucene.util.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

final class UnsafeHistogram
{
    private UnsafeHistogram()
    {
    }

    // TODO: count parallel heuristic for large inputs
    private static void count(Object inputBase, long inputAddress, int inputSize, int[] counts)
    {
        long input = inputAddress;

        Arrays.fill(counts, 0);

        for (int i = 0; i < inputSize; i++) {
            int symbol = UNSAFE.getByte(inputBase, input) & 0xFF;
            input++;
            counts[symbol]++;
        }
    }

    public static int findLargestCount(int[] counts, int maxSymbol)
    {
        int max = 0;
        for (int i = 0; i <= maxSymbol; i++) {
            if (counts[i] > max) {
                max = counts[i];
            }
        }

        return max;
    }

    public static int findMaxSymbol(int[] counts, int maxSymbol)
    {
        while (counts[maxSymbol] == 0) {
            maxSymbol--;
        }
        return maxSymbol;
    }

    public static void count(byte[] input, int length, int[] counts)
    {
        count(input, ARRAY_BYTE_BASE_OFFSET, length, counts);
    }
}