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

    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // We store the reference as a VInt
        int minValue = IntegerCompressionUtils.getMinValue(ints);
        int maxValue = IntegerCompressionUtils.getMaxValue(ints);

        HashMap<Integer, List<Integer>> bitsNeededCount = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            int bitsRequired = PackedInts.bitsRequired(ints[i] - minValue);
            if (!bitsNeededCount.containsKey(bitsRequired)) {
                bitsNeededCount.put(bitsRequired, new ArrayList<>());
            }
            bitsNeededCount.get(bitsRequired).add(i);
        }

        // Bitmask for if the position index is an exception. 1 is exception. 128 bits total
        // We only use it if we have more than 16 exceptions since otherwise it isn't worth it
        byte[] exceptionBitMask = new byte[16];

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


        List<Integer> exceptionIndices = new ArrayList<>();
        List<Integer> exceptionValues = new ArrayList<>();
        for (int i = 32; i > 0; i--) {
            if (bitsNeededCount.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitsNeededCount.get(i))
                    {
                        IntegerCompressionUtils.setNthBit(exceptionBitMask, index);
                        exceptionIndices.add(index);
                        exceptionValues.add(ints[index] - minValue);
                    }
                }
                else {
                    break;
                }
            }
        }

        byte exceptionCount = (byte)exceptionIndices.size();

        out.writeVInt(minValue);
        out.writeByte((byte)bestBitWidth);
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

        boolean useBitmask = exceptionCount > 100;
        if (useBitmask) {
            out.writeBytes(exceptionBitMask, 0, 16);
        }

        int count = 0;
        // Now the exceptions Lists
        for (int index : exceptionIndices)
        {
            if (!useBitmask)
                out.writeByte((byte)index);
            out.writeVInt(IntegerCompressionUtils.getLeftBits(exceptionValues.get(count), 32 - bestBitWidth));
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
        byte regularValueBitWidth = pdu.in.readByte();

        byte exceptionCount = pdu.in.readByte();
        byte[] exceptionBitMask = new byte[16];
        boolean useBitmask = exceptionCount > 100;

        ForUtil forUtil = new ForUtil();

        //forUtil.decode(regularValueBitWidth, pdu, ints);
        LimitTestCompressor.decode(regularValueBitWidth, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
        }

        if (useBitmask)
            pdu.in.readBytes(exceptionBitMask, 0, 16);

        if (!useBitmask) {
            for (int i = 0; i < exceptionCount; i++) {
                byte index = pdu.in.readByte();
                int value = pdu.in.readVInt();
                value = value << regularValueBitWidth;
                ints[index] += value;
                //ints[index] = value + minValue;
            }
        } else {
            for (int i = 0; i < 128; i++) {
                if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                    int value = pdu.in.readVInt();
                    value = value << regularValueBitWidth;
                    ints[i] += value;
                }
            }
        }

        //IntegerCompressionUtils.turnAbsolutesIntoDeltas(ints);
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        int minValue = in.readVInt();
        byte regularValueBitWidth = in.readByte();

        byte exceptionCount = in.readByte();
        ForUtil forUtil = new ForUtil();
        boolean useBitmask = exceptionCount > 100;
        byte[] exceptionBitMask = new byte[16];
        // Calculate the total number of bytes required
        int totalBits = 128 * regularValueBitWidth;
        int totalBytes = (totalBits + 7) / 8; // Round up to the nearest byte
        in.skipBytes(totalBytes);

        if (useBitmask)
            in.readBytes(exceptionBitMask, 0, 16);

        //in.skipBytes(ForUtil.numBytes(regularValueBitWidth));

        if (!useBitmask) {
            for (int i = 0; i < exceptionCount; i++) {
                in.readByte();
                in.readVInt();
                //ints[index] = value + minValue;
            }
        } else {
            for (int i = 0; i < 128; i++) {
                if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                    in.readVInt();
                }
            }
        }

    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
