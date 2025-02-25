package org.apache.lucene.codecs.integercompression;


import org.apache.lucene.codecs.lucene912.*;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class LuceneDefaultCompressor implements IntegerCompressor {

    @Override
    public void encode(long[] deltas, DataOutput out) throws IOException {
        final ForUtil forUtil = new ForUtil();
        PForUtil pforUtil = new PForUtil(forUtil);
        pforUtil.encode(deltas, out);
    }

    @Override
    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeVInt(input);
    }

    @Override
    public int decodeSingleInt(IndexInput input) throws IOException {
        return input.readVInt();
    }

    @Override
    public void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        final ForUtil forUtil = new ForUtil();
        PForUtil pforUtil = new PForUtil(forUtil);
        pforUtil.decode(pdu, longs);
    }

    @Override
    public IntegerCompressionType getType() {
        return null;
    }
}
