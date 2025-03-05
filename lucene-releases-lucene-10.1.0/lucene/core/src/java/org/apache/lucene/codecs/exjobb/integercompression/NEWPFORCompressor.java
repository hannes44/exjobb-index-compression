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
public class NEWPFORCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] ints, DataOutput out) throws IOException
    {
        final int MAX_EXCEPTIONS = 32;
        //IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);

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
                 //- bitsSavedFromMinValueReference;


                int bitsRequired = (i * (128 - totalExceptions) + totalExceptions * 16);
                if (minBitsRequired > bitsRequired)
                {
                    minBitsRequired = bitsRequired;
                    bestBitWidth = i;
                }
                totalExceptions += bitsNeededCount.get(i).size();
            }
        }

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
                    }
                }
                else {
                    break;
                }
            }
        }

        int exceptionCount = exceptionIndices.size();

        out.writeVInt(minValue);
        out.writeVInt(bestBitWidth);
        out.writeInt(exceptionCount);

        ForUtil forUtil = new ForUtil();

        // Now store the offsets from the reference
        for (int i = 0; i < 128; i++) {
            ints[i] = ints[i] - minValue;
        }

        // We encode all ints with the best bit width. The exceptions will still be missing some bits so we add them in two lists after.
        // One list with the index of each exception and one list with the reminding value.
        forUtil.encode(ints, bestBitWidth, out);



        int count = 0;
        // Now the exceptions Lists
        for (int index : exceptionIndices)
        {
            out.writeVInt(index);
            out.writeVInt(IntegerCompressionUtils.getLeftBits(exceptionValues.get(count), bestBitWidth));
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
    public void decode(PostingDecodingUtil pdu, int[] ints) throws IOException {
        int minValue = pdu.in.readVInt();
        int maxBits = pdu.in.readVInt();
        int exceptionCount = pdu.in.readInt();
        ForUtil forUtil = new ForUtil();

        forUtil.decode(maxBits, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
        }

        for (int i = 0; i < exceptionCount; i++) {
            int index = pdu.in.readVInt();
            int value = pdu.in.readVInt();
            ints[index] += value;
        }

        //IntegerCompressionUtils.turnAbsolutesIntoDeltas(ints);
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
