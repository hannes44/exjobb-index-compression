package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Implements PFOR compression for integer sequences. Not optimized at all, should be very slow
 */
public class PFORCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code int} into {@code out}. */
    public void encode(int[] positions, DataOutput out) throws IOException
    {
     //   IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);

        // We store the reference as a VInt
        int minValue = IntegerCompressionUtils.getMinValue(positions);
        int maxValue = IntegerCompressionUtils.getMaxValue(positions);

        int maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);

        // Bitmask for if the position index is an exception. 1 is exception. 128 bits total
        byte[] exceptionBitMask = new byte[16];
        Arrays.fill(exceptionBitMask, (byte) 0); // Clear the bitmask

        HashMap<Integer, List<Integer>> bitsNeededCount = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            int bitsRequired = PackedInts.bitsRequired(positions[i] - minValue);
            if (!bitsNeededCount.containsKey(bitsRequired)) {
                bitsNeededCount.put(bitsRequired, new ArrayList<>());
            }
            bitsNeededCount.get(bitsRequired).add(i);
        }

        int totalExceptions = 0;
        int minBitsRequired = maxBitsRequired * 128;
        int bestBitWidth = maxBitsRequired;
        for (int i = 32; i > 0; i--) {
            if (bitsNeededCount.containsKey(i)) {
                int bitsRequired = (i * (128 - totalExceptions) + totalExceptions * 16) + 128 * (totalExceptions > 0 ? 1 : 0);
                if (minBitsRequired > bitsRequired)
                {
                    minBitsRequired = bitsRequired;
                    bestBitWidth = i;
                }
                totalExceptions += bitsNeededCount.get(i).size();
            }
        }

        for (int i = 32; i > 0; i--) {
            if (bitsNeededCount.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitsNeededCount.get(i))
                    {
                        IntegerCompressionUtils.setNthBit(exceptionBitMask, index);
                    }
                }
                else {
                    break;
                }
            }
        }


        // The first bit in the output is a flag for if we have any exceptions. This saves 127 bits for cases where there are no exceptions
        // Currently doing it in a byte for simplicity but it should be encoded into the minvalue for maximum gain
        byte isThereExceptions = (maxBitsRequired != bestBitWidth) ? (byte) 1 : (byte) 0;
        out.writeByte(isThereExceptions);
        out.writeVInt(minValue);
        out.writeVInt(bestBitWidth);

        if (isThereExceptions == 1)
            out.writeBytes(exceptionBitMask, 0, 16);


        List<Integer> regularValues = new ArrayList<>();
        List<Integer> exceptionValues = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                exceptionValues.add(positions[i] - minValue);
            }
            else {
                regularValues.add(positions[i] - minValue);
            }
        }


        byte[] regularBytes = LimitTestCompressor.bitPack(regularValues, bestBitWidth);

        // Only write the regular len if there is exceptions
        if (isThereExceptions != 0)
            out.writeVInt(regularBytes.length);

        out.writeBytes(regularBytes, regularBytes.length);

        for (Integer exception : exceptionValues)
        {
            out.writeVInt(exception);
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
        byte isThereExceptions = pdu.in.readByte();
        int minValue = pdu.in.readVInt();
        int regularBitWidth = pdu.in.readVInt();
        byte[] exceptionBitMask = new byte[16];

        // There is only an exceptionBitMask if there exists exceptions
        if (isThereExceptions == 1)
            pdu.in.readBytes(exceptionBitMask, 0, 16);

        // If there is no exceptions, we can figure out how many bytes there are
        int regularBytesLen = (((regularBitWidth * 128) + 7) / 8 * 8) / 8;

        if (isThereExceptions != 0)
            regularBytesLen = pdu.in.readVInt();



        byte[] regularBytes = new byte[regularBytesLen];
        pdu.in.readBytes(regularBytes, 0, regularBytesLen);

        List<Integer> regularValues = LimitTestCompressor.bitUnpack(regularBytes, regularBitWidth);

        int regularValueCount = 0;
        for (int i = 0; i < 128; i++) {
            if (isThereExceptions == 0 || IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 0) {
                ints[i] = regularValues.get(regularValueCount) + minValue;
                regularValueCount++;
            }
            else {
                ints[i] = pdu.in.readVInt() + minValue;
            }
        }

  //      IntegerCompressionUtils.turnAbsolutesIntoDeltas(ints);
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.PFOR;
    }
}
