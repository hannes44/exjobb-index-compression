/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

interface UnsafeBlockCompressor
{
    UnsafeBlockCompressor UNSUPPORTED = (inputBase, inputAddress, inputSize, unsafeSequenceStore, unsafeBlockCompressionState, offsets, parameters) -> { throw new UnsupportedOperationException(); };

    int compressBlock(Object inputBase, long inputAddress, int inputSize, UnsafeSequenceStore output, UnsafeBlockCompressionState state, UnsafeRepeatedOffsets offsets, UnsafeCompressionParameters parameters);
}