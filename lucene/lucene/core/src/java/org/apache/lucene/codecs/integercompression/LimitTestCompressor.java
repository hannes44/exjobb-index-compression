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

/**
 * idk, Doesn't work with the current bitpacking library, bitpacking for n longs is needed
 */
public class LimitTestCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        // We store the reference as a VInt
        long minValue = positions[0];
        long maxValue = positions[0];
        long totalBitsRequired = 0;
        long averageBitsRequired = 0;



        for (int i = 0; i < 128; i++) {
            long bitsRequired = PackedInts.bitsRequired(positions[i]);
            totalBitsRequired += bitsRequired;

            if (positions[i] > maxValue)
            {
                maxValue = positions[i];
            }
            if (positions[i] < minValue)
            {
                minValue = positions[i];
            }
        }

        averageBitsRequired = totalBitsRequired / 128;
        long maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);
        long useAverageValueForEncodingBitmask1 = 0;
        long useAverageValueForEncodingBitmask2 = 0;

        ArrayList<Long> averageBitLongs = new ArrayList<Long>();
        ArrayList<Long> maxBitLongs = new ArrayList<Long>();

        for (int i = 0; i < 128; i++) {
            long bitsRequired = PackedInts.bitsRequired(positions[i]);
            if (bitsRequired < averageBitsRequired)
            {
                if (i <= 63)
                    setNthBit(useAverageValueForEncodingBitmask1, i);
                else
                    setNthBit(useAverageValueForEncodingBitmask2, i-64);
                averageBitLongs.add(positions[i] - minValue);
            }
            else {
                maxBitLongs.add(positions[i] - minValue);
            }
        }

        out.writeVLong(minValue);
        // Could store the delta between max and average instead to save a couple bits
        out.writeVLong(maxBitsRequired);
        out.writeVLong(averageBitsRequired);

        out.writeLong(useAverageValueForEncodingBitmask1);
        out.writeLong(useAverageValueForEncodingBitmask2);

        ForUtil forUtil = new ForUtil();



        // Now store the offsets from the reference
      //  for (int i = 0; i < 128; i++) {
      //      positions[i] = positions[i] - minValue;
      //  }

        long[] avgBitsLongs = new long[averageBitLongs.size()];
        long[] maxBitsLongs = new long[maxBitLongs.size()];

        int n = 0;
        for (Long x : averageBitLongs)
        {
           long xd = x;
            avgBitsLongs[n] = xd;
           n++;
        }

        n = 0;
        for (Long x : maxBitLongs)
        {
            long xd = x;
            maxBitsLongs[n] = xd;
            n++;
        }

        forUtil.encode(avgBitsLongs, (int) averageBitsRequired, out);
        forUtil.encode(maxBitsLongs, (int) maxBitsRequired, out);
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
        long averageBits = pdu.in.readVLong();
        long useAverageValueForEncodingBitmask1 = pdu.in.readLong();
        long useAverageValueForEncodingBitmask2 = pdu.in.readLong();

        int totalAverageLongs = countOneBits(useAverageValueForEncodingBitmask1) + countOneBits(useAverageValueForEncodingBitmask2);

        ForUtil forUtil = new ForUtil();

        long[] averageLongs = new long[totalAverageLongs];
        forUtil.decode((int)averageBits, pdu, averageLongs);

        long[] maxLongs = new long[128-totalAverageLongs];
        forUtil.decode((int)maxBits, pdu, maxLongs);

        int averageCount = 0;
        int maxCount = 0;
        for (int i = 0; i < 128; i++) {
            if (i <= 63)
            {
                boolean isAverageLong = 1 == getNthBit(useAverageValueForEncodingBitmask1, i);
                if (isAverageLong)
                {
                    longs[i] = averageLongs[averageCount] + minValue;
                    averageCount++;
                }
                else {
                    longs[i] = maxLongs[maxCount] + minValue;
                    maxCount++;
                }
            }
            else {

                boolean isAverageLong = 1 == getNthBit(useAverageValueForEncodingBitmask1, i-64);
                if (isAverageLong)
                {
                    longs[i] = averageLongs[averageCount] + minValue;
                    averageCount++;
                }
                else {
                    longs[i] = maxLongs[maxCount] + minValue;
                    maxCount++;
                }
            }


        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }

    /**
     * Sets the n-th bit to 1 in a long value.
     *
     * @param value The original long value.
     * @param n     The bit position to set (0-based index, 0 = LSB, 63 = MSB).
     * @return The modified long value with the n-th bit set to 1.
     * @throws IllegalArgumentException if n is not in the range [0, 63].
     */
    public static long setNthBit(long value, int n) {
        // Validate the bit position
        if (n < 0 || n > 63) {
            throw new IllegalArgumentException("Bit position must be between 0 and 63.");
        }

        // Create a mask with the n-th bit set to 1
        long mask = 1L << n;

        // Use bitwise OR to set the n-th bit
        return value | mask;
    }

    /**
     * Looks up the n-th bit in a long value.
     *
     * @param value The long value to check.
     * @param n     The bit position to look up (0-based index, 0 = LSB, 63 = MSB).
     * @return 1 if the n-th bit is set, 0 otherwise.
     * @throws IllegalArgumentException if n is not in the range [0, 63].
     */
    public static int getNthBit(long value, int n) {
        // Validate the bit position
        if (n < 0 || n > 63) {
            throw new IllegalArgumentException("Bit position must be between 0 and 63.");
        }

        // Create a mask with the n-th bit set to 1
        long mask = 1L << n;

        // Use bitwise AND to check if the n-th bit is set
        return (value & mask) != 0 ? 1 : 0;
    }

    /**
     * Counts the number of 1 bits in a long value.
     *
     * @param value The long value to count the 1 bits in.
     * @return The number of 1 bits in the value.
     */
    public static int countOneBits(long value) {
        int count = 0;
        while (value != 0) {
            // Clear the least significant 1 bit
            value = value & (value - 1);
            count++;
        }
        return count;
    }

}
