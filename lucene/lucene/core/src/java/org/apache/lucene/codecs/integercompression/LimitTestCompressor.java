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
            long bitsRequired = PackedInts.bitsRequired(positions[i] - minValue);
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
        out.writeByte((byte) maxBitsRequired);
        out.writeByte((byte) averageBitsRequired);

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

    //    forUtil.encode(avgBitsLongs, (int) averageBitsRequired, out);
        byte[] avgBytes = bitPack(averageBitLongs,(int) averageBitsRequired);

        //forUtil.encode(maxBitsLongs, (int) maxBitsRequired, out);
        byte[] maxBytes = bitPack(maxBitLongs, (int) maxBitsRequired);
        //ArrayList<Long> averageBitLongs2 = (ArrayList<Long>) bitUnpack(avgBytes, (int) averageBitsRequired);
        //ArrayList<Long> maxBitLongs2 = (ArrayList<Long>) bitUnpack(maxBytes, (int) maxBitsRequired);

     //   out.writeVInt(avgBytes.length);
     //   out.writeVInt(maxBytes.length);
        out.writeBytes(avgBytes, avgBytes.length);
        out.writeBytes(maxBytes, maxBytes.length);
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
        int bytesNeededForAvg = pdu.in.readVInt();
        int bytesNeededForMax = pdu.in.readVInt();
        byte[] avgBytes = new byte[bytesNeededForAvg];
        pdu.in.readBytes(avgBytes, 0, bytesNeededForAvg);


        byte[] maxBytes = new byte[bytesNeededForMax];
        pdu.in.readBytes(maxBytes, 0, bytesNeededForMax);

        List<Long> averageLongs = bitUnpack(avgBytes,(int) averageBits);
        List<Long> maxLongs = bitUnpack(maxBytes,(int) maxBits);

       // forUtil.decode((int)averageBits, pdu, averageLongs);

     //   long[] maxLongs = new long[128-totalAverageLongs];
     //   forUtil.decode((int)maxBits, pdu, maxLongs);

        int averageCount = 0;
        int maxCount = 0;
        for (int i = 0; i < 128; i++) {
            if (i <= 63)
            {
                boolean isAverageLong = 1 == getNthBit(useAverageValueForEncodingBitmask1, i);
                if (isAverageLong)
                {
                    longs[i] = averageLongs.get(averageCount) + minValue;
                    averageCount++;
                }
                else {
                    longs[i] = maxLongs.get(maxCount) + minValue;
                    maxCount++;
                }
            }
            else {

                boolean isAverageLong = 1 == getNthBit(useAverageValueForEncodingBitmask1, i-64);
                if (isAverageLong)
                {
                    longs[i] = averageLongs.get(averageCount) + minValue;
                    averageCount++;
                }
                else {
                    longs[i] = maxLongs.get(maxCount) + minValue;
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


    /**
     * Writes a byte into the output array at the specified index.
     *
     * @param output The byte array to write into.
     * @param index  The index in the array to write the byte.
     * @param value  The byte value to write.
     */
    private static void writeByte(byte[] output, int index, byte value) {
        output[index] = value;
    }

    /**
     * Bit-packs a list of longs into a byte array.
     *
     * @param values The list of long values to pack.
     * @param bits   The number of bits to use for each value.
     * @return The packed byte array.
     * @throws IllegalArgumentException if bits is not in the range [1, 64].
     */
    public static byte[] bitPack(List<Long> values, int bits) {
        if (bits < 1 || bits > 64) {
            throw new IllegalArgumentException("Bits must be between 1 and 64.");
        }

        // Calculate the total number of bytes required
        int totalBits = values.size() * bits;
        int totalBytes = (totalBits + 7) / 8; // Round up to the nearest byte
        byte[] output = new byte[totalBytes];

        int bitIndex = 0; // Tracks the current bit position in the output array

        for (long value : values) {
            // Mask to extract the lower 'bits' bits
            long mask = (1L << bits) - 1;
            long packedValue = value & mask;

            // Write the bits into the output array
            for (int i = 0; i < bits; i++) {
                if ((packedValue & (1L << i)) != 0) {
                    int byteIndex = bitIndex / 8;
                    int bitOffset = bitIndex % 8;
                    output[byteIndex] |= (1 << bitOffset); // Set the bit
                }
                bitIndex++;
            }
        }

        return output;
    }

    /**
     * Reads a bit from the byte array at the specified bit index.
     *
     * @param bytes    The byte array to read from.
     * @param bitIndex The index of the bit to read.
     * @return The value of the bit (0 or 1).
     */
    private static int readBit(byte[] bytes, int bitIndex) {
        int byteIndex = bitIndex / 8;
        int bitOffset = bitIndex % 8;
        return (bytes[byteIndex] >> bitOffset) & 1;
    }

    /**
     * Unpacks a byte array into a list of longs.
     *
     * @param bytes The byte array to unpack.
     * @param bits  The number of bits used for each value.
     * @return The list of unpacked long values.
     * @throws IllegalArgumentException if bits is not in the range [1, 64].
     */
    public static List<Long> bitUnpack(byte[] bytes, int bits) {
        if (bits == 0)
            return new ArrayList<>();

        if (bits < 1 || bits > 64) {
            throw new IllegalArgumentException("Bits must be between 1 and 64.");
        }

        List<Long> values = new ArrayList<>();
        int bitIndex = 0; // Tracks the current bit position in the byte array

        while (bitIndex + bits <= bytes.length * 8) {
            long value = 0;
            for (int i = 0; i < bits; i++) {
                // Read the bit and add it to the value
                value |= (long) readBit(bytes, bitIndex) << i;
                bitIndex++;
            }
            values.add(value);
        }

        return values;
    }
}
