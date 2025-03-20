/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;


import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.MAX_BLOCK_SIZE;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.checkArgument;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.clamp;

class UnsafeCompressionContext
{
    public final UnsafeCompressionParameters parameters;
    public final UnsafeRepeatedOffsets offsets = new UnsafeRepeatedOffsets();
    public final UnsafeBlockCompressionState unsafeBlockCompressionState;
    public final UnsafeSequenceStore unsafeSequenceStore;

    public final UnsafeSequenceEncodingContext unsafeSequenceEncodingContext = new UnsafeSequenceEncodingContext();

    public final UnsafeHuffmanCompressionContext huffmanContext = new UnsafeHuffmanCompressionContext();

    public UnsafeCompressionContext(UnsafeCompressionParameters parameters, long baseAddress, int inputSize)
    {
        this.parameters = parameters;

        int windowSize = clamp(inputSize, 1, parameters.getWindowSize());
        int blockSize = Math.min(MAX_BLOCK_SIZE, windowSize);
        int divider = (parameters.getSearchLength() == 3) ? 3 : 4;

        int maxSequences = blockSize / divider;

        unsafeSequenceStore = new UnsafeSequenceStore(blockSize, maxSequences);

        unsafeBlockCompressionState = new UnsafeBlockCompressionState(parameters, baseAddress);
    }

    public void slideWindow(int slideWindowSize)
    {
        checkArgument(slideWindowSize > 0, "slideWindowSize must be positive");
        unsafeBlockCompressionState.slideWindow(slideWindowSize);
    }

    public void commit()
    {
        offsets.commit();
        huffmanContext.saveChanges();
    }
}