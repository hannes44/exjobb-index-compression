package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.codecs.lucene101.PForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/** Default Compressor for lucene 10.1.0 */
public class LuceneDefaultCompressor implements IntegerCompressor {

    @Override
    public void encode(int[] positions, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
       // for ()

        final ForUtil forUtil = new ForUtil();
        PForUtil pforUtil = new PForUtil();
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
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
        final ForUtil forUtil = new ForUtil();
        PForUtil pforUtil = new PForUtil();
        pforUtil.decode(pdu, ints);
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        PForUtil pforUtil = new PForUtil();
        PForUtil.skip(in);
    }

    @Override
    public IntegerCompressionType getType() {
        return null;
    }
}
