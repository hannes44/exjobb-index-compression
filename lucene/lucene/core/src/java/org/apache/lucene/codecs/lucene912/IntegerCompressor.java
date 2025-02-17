package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public interface IntegerCompressor {
    public void encode(long[] deltas, DataOutput out) throws IOException;

    public void encodeSingleInt(int input, DataOutput out) throws IOException;

    public int decodeSingleInt(IndexInput input) throws IOException;

    public void decode(PostingDecodingUtil pdu, long[] longs) throws IOException;

    public IntegerCompressionType getType();
}
