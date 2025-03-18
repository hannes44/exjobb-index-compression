package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.*;

/**
 * Implements FOR compression for integer sequences.
 */
public class FASTPFORCompressor implements IntegerCompressor {

    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // We store the reference as a VInt
        int minValue = IntegerCompressionUtils.getMinValue(ints);
        int maxValue = IntegerCompressionUtils.getMaxValue(ints);

        // TODO: Stores the bits needed for each int. This does not consider that we will remove the bits required from the minValue
        HashMap<Integer, List<Integer>> bitsNeededCount = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            int bitsRequired = PackedInts.bitsRequired(ints[i] - minValue);
            if (!bitsNeededCount.containsKey(bitsRequired)) {
                bitsNeededCount.put(bitsRequired, new ArrayList<>());
            }
            bitsNeededCount.get(bitsRequired).add(i);
        }

        // int bitsSavedFromMinValueReference = PackedInts.bitsRequired(minValue);
        int maxBitsRequired = PackedInts.bitsRequired(maxValue-minValue);

        int totalExceptions = 0;
        int minBitsRequired = maxBitsRequired * 128;
        int bestBitWidth = maxBitsRequired;
        for (int i = 32; i > 0; i--) {
            if (bitsNeededCount.containsKey(i)) {
                int bitsRequired = (i * (128 - totalExceptions) + totalExceptions * 16);
                if (minBitsRequired > bitsRequired)
                {
                    minBitsRequired = bitsRequired;
                    bestBitWidth = i;
                }
                totalExceptions += bitsNeededCount.get(i).size();
            }
        }

        int maxException = 0;
        List<Integer> exceptionIndices = new ArrayList<>();
        List<Integer> exceptionValues = new ArrayList<>();
        for (int i = 32; i > 0; i--) {
            if (bitsNeededCount.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitsNeededCount.get(i))
                    {
                        exceptionIndices.add(index);
                        exceptionValues.add(ints[index] - minValue);

                        if (maxException < ints[index] - minValue) {
                            maxException = ints[index] - minValue;
                        }
                    }
                }
                else {
                    break;
                }
            }
        }

        byte exceptionCount = (byte)exceptionIndices.size();

        int exceptionBitCount = PackedInts.bitsRequired(maxException) - bestBitWidth;

        out.writeVInt(minValue);
        out.writeVInt(bestBitWidth);
        out.writeByte(exceptionCount);

        ForUtil forUtil = new ForUtil();

        // Now store the offsets from the reference
        for (int i = 0; i < 128; i++) {
            ints[i] = ints[i] - minValue;
        }

        // We encode all ints with the best bit width. The exceptions will still be missing some bits so we add them in two lists after.
        // One list with the index of each exception and one list with the reminding value.
        //forUtil.encode(ints, bestBitWidth, out);
        LimitTestCompressor.encode(ints, bestBitWidth, out);

        if (exceptionIndices.size() == 0)
            return;

        out.writeVInt(exceptionBitCount);

        // The position in the exception list
        out.writeVInt(exceptions.get(exceptionBitCount).size());

        int count = 0;
        // Now the exceptions Lists
        for (int index : exceptionIndices)
        {
            out.writeVInt(index);
            int leftExceptionBits = IntegerCompressionUtils.getLeftBits(exceptionValues.get(count), 32 - bestBitWidth);
            exceptions.get(exceptionBitCount).add(leftExceptionBits);
            //out.writeVInt(leftExceptionBits);
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
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
        int minValue = pdu.in.readVInt();
        int regularValueBitWidth = pdu.in.readVInt();

        byte exceptionCount = pdu.in.readByte();
        ForUtil forUtil = new ForUtil();

        //forUtil.decode(regularValueBitWidth, pdu, ints);
        LimitTestCompressor.decode(regularValueBitWidth, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
        }

        if (exceptionCount == 0)
            return;

        int exceptionBitCount = pdu.in.readVInt();

        int exceptionIndexStart = pdu.in.readVInt();

        int exceptionIndex = exceptionIndexStart;

        for (int i = 0; i < exceptionCount; i++) {
            int index = pdu.in.readVInt();

            ints[index] += exceptions.get(exceptionBitCount).get(exceptionIndex) << regularValueBitWidth;
            exceptionIndex++;
        }

    }

    @Override
    public void skip(IndexInput in) throws IOException {

    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
