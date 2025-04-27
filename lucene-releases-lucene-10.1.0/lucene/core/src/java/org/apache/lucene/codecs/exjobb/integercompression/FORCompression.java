package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.codecs.lucene101.PForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Implements FOR compression for integer sequences.
 */
public final class FORCompression implements IntegerCompressor {
    ForUtil forUtil = new ForUtil();
    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        MinMax minMax = IntegerCompressionUtils.findMinMax(ints);
        int minValue = minMax.min();
        int maxValue = minMax.max();

        int bitWidth = PackedInts.bitsRequired(maxValue);

        // To most significant bit is flag for if all vales are equal, the rest 7 bits are the bitwidth
        byte token = (byte)bitWidth;

        // If all values are the same, we can save many bits
        if (minValue == maxValue) {
            token = (byte) (token | 0x80);
            out.writeByte(token);
            out.writeVInt(ints[0]);
        } else {
            out.writeByte(token);
            forUtil.encode(ints, token, out);
        }
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /**
     * Delta Decode 128 integers into {@code ints}.
     *
     * @return
     */
    public boolean decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions, short[] shorts) throws IOException {
        byte token = pdu.in.readByte();
        int bitWidth = (byte) (token & 0b01111111);
        int isAllEqual = (byte) (token & 0b10000000);

        if (isAllEqual == 0) {
            forUtil.decode(bitWidth, pdu, ints);
        }
        else {
            Arrays.fill(ints, 0, ForUtil.BLOCK_SIZE, pdu.in.readVInt());
        }
        return false;
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
        int token = in.readByte();
        int bitWidth = (byte) (token & 0b01111111);
        int isAllEqual = (byte) (token & 0b10000000);
        //     int minValue = in.readVInt();
        if (isAllEqual == 0)
            in.skipBytes(ForUtil.numBytes(bitWidth));
        else
            in.readVInt();
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
