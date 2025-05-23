package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.PForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/** Default Compressor for lucene 10.1.0 */
public class LuceneDefaultCompressor implements IntegerCompressor {
    private final PForUtil pforUtil = new PForUtil();
    @Override
    public void encode(int[] positions, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {

        pforUtil.encode(positions, out);
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
    public boolean decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions, short[] shorts) throws IOException {
        pforUtil.decode(pdu, ints);
        return false;
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        PForUtil.skip(in);
    }

    @Override
    public IntegerCompressionType getType() {
        return null;
    }
}
