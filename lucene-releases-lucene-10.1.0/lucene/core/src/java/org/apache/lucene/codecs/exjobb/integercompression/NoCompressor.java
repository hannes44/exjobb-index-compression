package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implements no compression for integer sequences.
 */
public class NoCompressor implements IntegerCompressor {
    ForUtil forUtil = new ForUtil();

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // We have to turn the given deltas into absolutes. This will cause some performance hits so for a proper test absolutes should be given instead
       // IntegerCompressionUtils.turnDeltasIntoAbsolutes(ints);
        for (int i = 0; i < 128; i++) {
            out.writeInt(ints[i]);
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
    /**
     * Delta Decode 128 integers into {@code ints}.
     *
     * @return
     */
    public boolean decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions, short[] shorts) throws IOException {
        pdu.in.readInts(ints, 0, 128);
        //IntegerCompressionUtils.turnAbsolutesIntoDeltas(ints);
        return false;
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        in.skipBytes(128*4);
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.NONE;
    }
}
