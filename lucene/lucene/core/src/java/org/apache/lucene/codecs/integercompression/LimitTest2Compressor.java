package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.ForUtil;
import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implements PFOR compression for integer sequences. Not optimized at all, should be very slow
 */
public class LimitTest2Compressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {

        // We store the reference as a VInt
        long minValue = IntegerCompressionUtils.getMinValue(positions);
        long maxValue = IntegerCompressionUtils.getMaxValue(positions);

        long maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);

        // Bitmask for if the position index is an exception. 1 is exception. 128 bits total
        byte[] exceptionBitMask = new byte[16];

        HashMap<Integer, List<Integer>> bitsNeededCount = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            int bitsRequired = PackedInts.bitsRequired(positions[i] - minValue);
            if (!bitsNeededCount.containsKey(bitsRequired)) {
                bitsNeededCount.put(bitsRequired, new ArrayList<>());
            }
            bitsNeededCount.get(bitsRequired).add(i);

        }

        int totalExceptions = 0;
        int minBitsRequired = 10000000;
        int bestBitWidth = (int) maxBitsRequired;
        for (int i = 64; i > 0; i--) {
            if (bitsNeededCount.containsKey(i)) {
               // int bitsRequired = (i * (128 - totalExceptions) + totalExceptions * i * 2) + 8 * (totalExceptions > 0 ? 1 : 0);
                int bitsRequired = (i * (128 - totalExceptions) + totalExceptions * 64) + 128 * (totalExceptions > 0 ? 1 : 0);
                if (minBitsRequired > bitsRequired)
                {
                    minBitsRequired = bitsRequired;
                    bestBitWidth = i;
                }
                totalExceptions += bitsNeededCount.get(i).size();
            }
        }

        int count = 0;
        for (int i = 64; i > 0; i--) {
            if (bitsNeededCount.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitsNeededCount.get(i))
                    {
                        IntegerCompressionUtils.setNthBit(exceptionBitMask, index);
                        count++;
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
     //   out.writeByte(isThereExceptions);
        out.writeVLong(minValue);
        out.writeVLong(bestBitWidth);

        if (isThereExceptions == 1)
            out.writeBytes(exceptionBitMask, 0, 16);


        List<Long> regularValues = new ArrayList<>();
        List<Long> exceptionValues = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
    //            exceptionValues.add(positions[i] - minValue);
                regularValues.add(positions[i] - minValue);
            }
            else {
                regularValues.add(positions[i] - minValue);
            }
        }


        byte[] regularBytes = LimitTestCompressor.bitPack(regularValues, bestBitWidth);

        //out.writeVInt(regularBytes.length);
        out.writeBytes(regularBytes, regularBytes.length);

        for (Long exception : exceptionValues)
        {
        //    out.writeLong(exception);
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
    public void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        byte isThereExceptions = pdu.in.readByte();
        long minValue = pdu.in.readVLong();
        long regularBitWidth = pdu.in.readVLong();
        byte[] exceptionBitMask = new byte[16];

        // There is only an exceptionBitMask if there exists exceptions
        if (isThereExceptions == 1)
            pdu.in.readBytes(exceptionBitMask, 0, 16);

        int regularBytesLen = pdu.in.readVInt();


        byte[] regularBytes = new byte[regularBytesLen];
        pdu.in.readBytes(regularBytes, 0, regularBytesLen);

        List<Long> regularValues = LimitTestCompressor.bitUnpack(regularBytes, (int) regularBitWidth);

        int regularValueCount = 0;
        for (int i = 0; i < 128; i++) {
            if (isThereExceptions == 0 || IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 0) {
                longs[i] = regularValues.get(regularValueCount) + minValue;
                regularValueCount++;
            }
            else {
                longs[i] = pdu.in.readLong() + minValue;
            }
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
