/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import java.util.Arrays;

class BlockCompressionState
{
    public final int[] hashTable;
    public final int[] chainTable;

    private final long baseAddress;

    // starting point of the window with respect to baseAddress
    private int windowBaseOffset;

    public BlockCompressionState(CompressionParameters parameters, long baseAddress)
    {
        this.baseAddress = baseAddress;
        hashTable = new int[1 << parameters.getHashLog()];
        chainTable = new int[1 << parameters.getChainLog()]; // TODO: chain table not used by Strategy.FAST
    }

    public void slideWindow(int slideWindowSize)
    {
        for (int i = 0; i < hashTable.length; i++) {
            int newValue = hashTable[i] - slideWindowSize;
            // if new value is negative, set it to zero branchless
            newValue = newValue & (~(newValue >> 31));
            hashTable[i] = newValue;
        }
        for (int i = 0; i < chainTable.length; i++) {
            int newValue = chainTable[i] - slideWindowSize;
            // if new value is negative, set it to zero branchless
            newValue = newValue & (~(newValue >> 31));
            chainTable[i] = newValue;
        }
    }

    public void reset()
    {
        Arrays.fill(hashTable, 0);
        Arrays.fill(chainTable, 0);
    }

    public void enforceMaxDistance(long inputLimit, int maxDistance)
    {
        int distance = (int) (inputLimit - baseAddress);

        int newOffset = distance - maxDistance;
        if (windowBaseOffset < newOffset) {
            windowBaseOffset = newOffset;
        }
    }

    public long getBaseAddress()
    {
        return baseAddress;
    }

    public int getWindowBaseOffset()
    {
        return windowBaseOffset;
    }
}