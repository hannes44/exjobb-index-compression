/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import static org.apache.lucene.util.compress.zstd.Huffman.MAX_FSE_TABLE_LOG;
import static org.apache.lucene.util.compress.zstd.Huffman.MAX_SYMBOL;
import static org.apache.lucene.util.compress.zstd.Huffman.MAX_TABLE_LOG;

class HuffmanTableWriterWorkspace
{
    // for encoding weights
    public final byte[] weights = new byte[MAX_SYMBOL]; // the weight for the last symbol is implicit

    // for compressing weights
    public final int[] counts = new int[MAX_TABLE_LOG + 1];
    public final short[] normalizedCounts = new short[MAX_TABLE_LOG + 1];
    public final FseCompressionTable fseTable = new FseCompressionTable(MAX_FSE_TABLE_LOG, MAX_TABLE_LOG);
}