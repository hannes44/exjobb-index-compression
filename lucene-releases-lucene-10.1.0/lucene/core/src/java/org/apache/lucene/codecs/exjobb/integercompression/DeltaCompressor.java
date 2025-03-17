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
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] positions, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        out.writeInt(positions[0]);

        for (int i = 1; i < positions.length; i++) {
            int delta = (positions[i] - positions[i-1]);
            out.writeInt(delta);
        }
    }

    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readInt();
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {

        ints[0] = pdu.in.readInt();
        for (int i = 1; i < 128; i++) {
            ints[i] = pdu.in.readInt() + ints[i-1];
        }
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        in.readInt();
        for (int i = 1; i < 128; i++) {
            in.readInt();
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.DELTA;
    }
}
