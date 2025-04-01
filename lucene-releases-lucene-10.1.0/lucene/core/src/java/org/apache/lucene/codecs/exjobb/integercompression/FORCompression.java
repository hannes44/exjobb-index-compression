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
public class FORCompression implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        //IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);

       int bitWidth = 10;

        // We store the reference as a VInt
        int minValue = ints[0];
        int maxValue = ints[0];
        for (int i = 0; i < 128; i++) {
            if (minValue > ints[i])
                minValue = ints[i];
            if (maxValue < ints[i])
                maxValue = ints[i];
        }

        int maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);
        out.writeVInt(bitWidth);
        out.writeVInt(minValue);
        out.writeVInt(maxBitsRequired);

        ForUtil forUtil = new ForUtil();



        ArrayList<Integer> rightBits = new ArrayList<>();

        final long maxUnpatchedValue = (1L << bitWidth) - 1;
        // Now store the offsets from the reference
        for (int i = 0; i < 128; i++) {
            ints[i] = ints[i] - minValue;
            rightBits.add(IntegerCompressionUtils.getLeftBits(ints[i], 32 - bitWidth));
            ints[i] &= maxUnpatchedValue;
        }
        forUtil.encode(ints, bitWidth, out);

        for (int i = 0; i < 128; i++) {
            out.writeInt(rightBits.get(i));
        }
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
        int bitWidth = pdu.in.readVInt();
        int minValue = pdu.in.readVInt();
        int maxBits = pdu.in.readVInt();
        ForUtil forUtil = new ForUtil();

        forUtil.decode(bitWidth, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
            int value = pdu.in.readInt();
            value = value << bitWidth;
            ints[i] += value;
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
        int bitWidth = in.readVInt();
        int minValue = in.readVInt();
        int maxBits = in.readVInt();
        ForUtil forUtil = new ForUtil();

        in.skipBytes(ForUtil.numBytes(bitWidth));
        for (int i = 0; i < 128; i++) {

            in.readInt();
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
