/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import java.util.Arrays;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_SYMBOL_COUNT;

class HuffmanCompressionTableWorkspace
{
    public final NodeTable nodeTable = new NodeTable((2 * MAX_SYMBOL_COUNT - 1)); // number of nodes in binary tree with MAX_SYMBOL_COUNT leaves

    public final short[] entriesPerRank = new short[Huffman.MAX_TABLE_LOG + 1];
    public final short[] valuesPerRank = new short[Huffman.MAX_TABLE_LOG + 1];

    // for setMaxHeight
    public final int[] rankLast = new int[Huffman.MAX_TABLE_LOG + 2];

    public void reset()
    {
        Arrays.fill(entriesPerRank, (short) 0);
        Arrays.fill(valuesPerRank, (short) 0);
    }
}
