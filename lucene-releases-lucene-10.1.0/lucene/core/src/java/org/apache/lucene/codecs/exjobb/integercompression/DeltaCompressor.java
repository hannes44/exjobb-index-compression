package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implements delta compression for integer sequences.
 */
public class DeltaCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}.
     *  Uses variable encoding on the deltas
     * */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // Since we are given the positions as deltas, we can simply write them to the output
        for (int i = 0; i < 128; i++) {
            out.writeVInt(ints[i]);
        }
    }

    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeVInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readVInt();
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {

        // Since we are returning the ints as deltas we can simply read them
        for (int i = 0; i < 128; i++) {
            ints[i] = pdu.in.readVInt();
        }
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        for (int i = 0; i < 128; i++) {
            in.readVInt();
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.DELTA;
    }
}
