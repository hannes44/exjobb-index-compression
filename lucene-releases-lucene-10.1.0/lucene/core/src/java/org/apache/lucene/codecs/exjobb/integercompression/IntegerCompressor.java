package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Interface for IntegerCompressor classes.
 */
public interface IntegerCompressor {
    public void encode(int[] deltas, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException;

    public void encodeSingleInt(int input, DataOutput out) throws IOException;

    public int decodeSingleInt(IndexInput input) throws IOException;

    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException;

    public void skip(IndexInput in) throws IOException;

    public IntegerCompressionType getType();
}
