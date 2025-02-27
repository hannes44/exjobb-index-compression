package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Interface for IntegerCompressor classes.
 */
public interface IntegerCompressor {
    public void encode(int[] deltas, DataOutput out) throws IOException;

    public void encodeSingleInt(int input, DataOutput out) throws IOException;

    public int decodeSingleInt(IndexInput input) throws IOException;

    public void decode(PostingDecodingUtil pdu, int[] ints) throws IOException;

    public IntegerCompressionType getType();
}
