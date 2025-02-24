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
import java.util.List;

/**
 * Implements FOR compression for integer sequences.
 */
public class PFORCompressor implements IntegerCompressor {

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

        // Testing with 1 exception
        IntegerCompressionUtils.setNthBit(exceptionBitMask, 75);

        out.writeVLong(minValue);
        out.writeVLong(maxBitsRequired);
        out.writeBytes(exceptionBitMask, 0, 16);


        List<Long> regularValues = new ArrayList<>();
        List<Long> exceptionValues = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 1) {
                exceptionValues.add(positions[i] - minValue);
            }
            else {
                regularValues.add(positions[i] - minValue);
            }
        }


        byte[] regularBytes = LimitTestCompressor.bitPack(regularValues, (int) maxBitsRequired);
        out.writeInt(regularBytes.length);
        out.writeBytes(regularBytes, regularBytes.length);

        for (Long exception : exceptionValues)
        {
            out.writeLong(exception);
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
        long minValue = pdu.in.readVLong();
        long maxBits = pdu.in.readVLong();
        byte[] exceptionBitMask = new byte[16];
        pdu.in.readBytes(exceptionBitMask, 0, 16);
        int regularBytesLen = pdu.in.readInt();


        byte[] regularBytes = new byte[regularBytesLen];
        pdu.in.readBytes(regularBytes, 0, regularBytesLen);

        List<Long> regularValues = LimitTestCompressor.bitUnpack(regularBytes, (int) maxBits);

        int regularValueCount = 0;
        for (int i = 0; i < 128; i++) {
            if (IntegerCompressionUtils.getNthBit(exceptionBitMask, i) == 0) {
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
