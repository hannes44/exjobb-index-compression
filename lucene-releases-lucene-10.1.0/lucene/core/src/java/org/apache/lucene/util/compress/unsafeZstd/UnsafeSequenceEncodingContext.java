/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.MAX_LITERALS_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.MAX_MATCH_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.MAX_OFFSET_CODE_SYMBOL;

class UnsafeSequenceEncodingContext
{
    private static final int MAX_SEQUENCES = Math.max(MAX_LITERALS_LENGTH_SYMBOL, MAX_MATCH_LENGTH_SYMBOL);

    public final UnsafeFseCompressionTable literalLengthTable = new UnsafeFseCompressionTable(UnsafeConstants.LITERAL_LENGTH_TABLE_LOG, MAX_LITERALS_LENGTH_SYMBOL);
    public final UnsafeFseCompressionTable offsetCodeTable = new UnsafeFseCompressionTable(UnsafeConstants.OFFSET_TABLE_LOG, MAX_OFFSET_CODE_SYMBOL);
    public final UnsafeFseCompressionTable matchLengthTable = new UnsafeFseCompressionTable(UnsafeConstants.MATCH_LENGTH_TABLE_LOG, MAX_MATCH_LENGTH_SYMBOL);

    public final int[] counts = new int[MAX_SEQUENCES + 1];
    public final short[] normalizedCounts = new short[MAX_SEQUENCES + 1];
}