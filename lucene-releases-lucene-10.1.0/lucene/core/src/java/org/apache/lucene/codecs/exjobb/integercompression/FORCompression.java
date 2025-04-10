package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Implements FOR compression for integer sequences.
 */
public final class FORCompression implements IntegerCompressor {
    ForUtil forUtil = new ForUtil();
    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {

        //IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);
        // We store the reference as a VInt
        int minValue = ints[0];
        int maxValue = ints[0];
        for (int i = 0; i < 128; i++) {
            if (minValue > ints[i])
                minValue = ints[i];
            if (maxValue < ints[i])
                maxValue = ints[i];
        }
        for (int i = 0; i < 128; i++) {
            ints[i] -= minValue;
        }

        int bitWidth = PackedInts.bitsRequired(maxValue - minValue);
        out.writeByte((byte) bitWidth);
        out.writeVInt(minValue);

        forUtil.encode(ints, bitWidth, out);
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
        int bitWidth = pdu.in.readByte();
        int minValue = pdu.in.readVInt();


        forUtil.decode(bitWidth, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
        }
    }


    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeVInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readVInt();
    }


    @Override
    public void skip(IndexInput in) throws IOException {
        int bitWidth = in.readByte();
        int minValue = in.readVInt();

        in.skipBytes(ForUtil.numBytes(bitWidth));
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
