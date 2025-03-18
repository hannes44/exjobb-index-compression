/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

interface BlockCompressor
{
    BlockCompressor UNSUPPORTED = (inputBase, inputAddress, inputSize, sequenceStore, blockCompressionState, offsets, parameters) -> { throw new UnsupportedOperationException(); };

    int compressBlock(Object inputBase, long inputAddress, int inputSize, SequenceStore output, BlockCompressionState state, RepeatedOffsets offsets, CompressionParameters parameters);
}