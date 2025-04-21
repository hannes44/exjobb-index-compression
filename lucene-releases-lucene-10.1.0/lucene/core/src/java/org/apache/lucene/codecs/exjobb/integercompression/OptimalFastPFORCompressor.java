package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.*;

/**
 * Implements FOR compression for integer sequences.
 */
public class OptimalFastPFORCompressor implements IntegerCompressor {

    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // We store the reference as a VInt
        //  int minValue = IntegerCompressionUtils.getMinValue(ints);
        int maxValue = IntegerCompressionUtils.getMaxValue(ints);
        // Bitmask for if the position index is an exception. 1 is exception. 128 bits total
        byte[] exceptionBitMask = new byte[16];

        HashMap<Integer, List<Integer>> bitCountToIndex = IntegerCompressionUtils.getBitCountToIndexMap(ints);

        // int bitsSavedFromMinValueReference = PackedInts.bitsRequired(minValue);
        int maxBitsRequired = PackedInts.bitsRequired(maxValue);


        int minBitsRequired = maxBitsRequired * 128;
        IntegerCompressionUtils.CostFunction costFunction = (bitWidth, totalExceptions, maxBitWidth) -> 128 + (totalExceptions * (maxBitWidth - bitWidth)) + bitWidth * 128;
        int bestBitWidth = IntegerCompressionUtils.getBestBitWidth(costFunction, maxBitsRequired, bitCountToIndex);

        int maxException = 0;
        List<Integer> exceptionValues = new ArrayList<>();
        for (int i = 32; i > 0; i--) {
            if (bitCountToIndex.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitCountToIndex.get(i))
                    {
                        IntegerCompressionUtils.setNthBit(exceptionBitMask, index);

                        if (maxException < ints[index]) {
                            maxException = ints[index];
                        }
                    }
                }
                else {
                    break;
                }
            }
        }

        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                exceptionValues.add(ints[i]);
            }
        }

        int exceptionBitCount = PackedInts.bitsRequired(maxException) - bestBitWidth;

        final long maxUnpatchedValue = (1L << bestBitWidth) - 1;
        for (int i = 0; i < 128; i++) {
            ints[i] &= maxUnpatchedValue;
        }

        ForUtil forUtil = new ForUtil();
        out.writeByte((byte)bestBitWidth);

        forUtil.encode(ints, bestBitWidth, out);

        if (exceptionValues.size() == 0)
            exceptionBitCount = 0;

        out.writeByte((byte)exceptionBitCount);
        if (exceptionBitCount == 0)
            return;

        out.writeBytes(exceptionBitMask, 0, 16);

        // The position in the exception list
        out.writeVInt(exceptions.get(exceptionBitCount).size());

        int count = 0;
        // Now the exceptions Lists
        for (int value : exceptionValues)
        {
            int leftExceptionBits = IntegerCompressionUtils.getLeftBits(value, 32 - bestBitWidth);
            exceptions.get(exceptionBitCount).add(leftExceptionBits);
            count++;
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
        byte[] exceptionBitMask = new byte[16];

        byte regularValueBitWidth = pdu.in.readByte();

        ForUtil forUtil = new ForUtil();
        forUtil.decode(regularValueBitWidth, pdu, ints);

        int exceptionBitCount = pdu.in.readByte();

        if (exceptionBitCount == 0)
            return false;

        pdu.in.readBytes(exceptionBitMask, 0, 16);

        int exceptionIndexStart = pdu.in.readVInt();

        int exceptionIndex = exceptionIndexStart;


        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                ints[i] += exceptions.get(exceptionBitCount).get(exceptionIndex) << regularValueBitWidth;
                exceptionIndex++;
            }
        }
        return false;
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        byte[] exceptionBitMask = new byte[16];

        int regularValueBitWidth = in.readByte();

        in.skipBytes(ForUtil.numBytes(regularValueBitWidth));

        // There is only an exceptionBitMask if there exists exceptions


        int exceptionBitCount = in.readByte();
        if (exceptionBitCount == 0)
            return;
        in.skipBytes(16);


        int exceptionIndexStart = in.readVInt();

    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
