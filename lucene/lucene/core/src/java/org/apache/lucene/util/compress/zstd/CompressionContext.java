/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;


import static org.apache.lucene.util.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.checkArgument;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.clamp;

class CompressionContext
{
    public final CompressionParameters parameters;
    public final RepeatedOffsets offsets = new RepeatedOffsets();
    public final BlockCompressionState blockCompressionState;
    public final SequenceStore sequenceStore;

    public final SequenceEncodingContext sequenceEncodingContext = new SequenceEncodingContext();

    public final HuffmanCompressionContext huffmanContext = new HuffmanCompressionContext();

    public CompressionContext(CompressionParameters parameters, long baseAddress, int inputSize)
    {
        this.parameters = parameters;

        int windowSize = clamp(inputSize, 1, parameters.getWindowSize());
        int blockSize = Math.min(MAX_BLOCK_SIZE, windowSize);
        int divider = (parameters.getSearchLength() == 3) ? 3 : 4;

        int maxSequences = blockSize / divider;

        sequenceStore = new SequenceStore(blockSize, maxSequences);

        blockCompressionState = new BlockCompressionState(parameters, baseAddress);
    }

    public void slideWindow(int slideWindowSize)
    {
        checkArgument(slideWindowSize > 0, "slideWindowSize must be positive");
        blockCompressionState.slideWindow(slideWindowSize);
    }

    public void commit()
    {
        offsets.commit();
        huffmanContext.saveChanges();
    }
}