/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

import java.util.Arrays;

class UnsafeNodeTable
{
    int[] count;
    short[] parents;
    int[] symbols;
    byte[] numberOfBits;

    public UnsafeNodeTable(int size)
    {
        count = new int[size];
        parents = new short[size];
        symbols = new int[size];
        numberOfBits = new byte[size];
    }

    public void reset()
    {
        Arrays.fill(count, 0);
        Arrays.fill(parents, (short) 0);
        Arrays.fill(symbols, 0);
        Arrays.fill(numberOfBits, (byte) 0);
    }

    public void copyNode(int from, int to)
    {
        count[to] = count[from];
        parents[to] = parents[from];
        symbols[to] = symbols[from];
        numberOfBits[to] = numberOfBits[from];
    }
}