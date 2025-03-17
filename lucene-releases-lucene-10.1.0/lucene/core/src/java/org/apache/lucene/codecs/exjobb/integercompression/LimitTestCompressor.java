package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * idk, Doesn't work with the current bitpacking library, bitpacking for n longs is needed
 */
public class LimitTestCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] positions, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        out.writeInt(positions[0]);

        for (int i = 1; i < positions.length; i++) {
            int delta = (positions[i] - positions[i-1]);
            out.writeInt(delta);
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
        ints[0] = pdu.in.readInt();
        for (int i = 1; i < 128; i++) {
            ints[i] = pdu.in.readInt() + ints[i-1];
        }
    }

    @Override
    public void skip(IndexInput in) throws IOException {

    }

    /** Encode 128 integers from {@code ints} into {@code out}. */
    public static void encode(int[] ints, int bitsPerValue, DataOutput out) throws IOException {
        if (ints.length != 128) {
            throw new IllegalArgumentException("Input array must have exactly 128 elements.");
        }
        if (bitsPerValue < 1 || bitsPerValue > 32) {
            throw new IllegalArgumentException("bitsPerValue must be between 1 and 32.");
        }

        // Mask to extract the lower `bitsPerValue` bits
        int mask = (1 << bitsPerValue) - 1;

        // Buffer to accumulate bits into bytes
        long buffer = 0; // Use a long to handle up to 64 bits
        int bufferLength = 0; // Number of bits currently in the buffer

        for (int value : ints) {
            // Extract the lower `bitsPerValue` bits
            int packedValue = value & mask;

            // Add the packed value to the buffer
            buffer = (buffer << bitsPerValue) | packedValue;
            bufferLength += bitsPerValue;

            // Write full bytes to the output
            while (bufferLength >= 8) {
                // Extract the top 8 bits from the buffer
                byte byteToWrite = (byte) (buffer >>> (bufferLength - 8));
                out.writeByte(byteToWrite);

                // Remove the written bits from the buffer
                bufferLength -= 8;
                buffer &= (1L << bufferLength) - 1; // Clear the top bits
            }
        }

        // Write any remaining bits (padding with zeros if necessary)
        if (bufferLength > 0) {
            // Shift the remaining bits to the top of the byte and write
            byte byteToWrite = (byte) (buffer << (8 - bufferLength));
            out.writeByte(byteToWrite);
        }
    }

    /** Decode 128 integers from {@code in} into {@code ints}. */
    public static void decode(int bitsPerValue, PostingDecodingUtil pdu, int[] ints) throws IOException {
        if (ints.length != 128) {
            throw new IllegalArgumentException("Output array must have exactly 128 elements.");
        }
        if (bitsPerValue < 1 || bitsPerValue > 32) {
            throw new IllegalArgumentException("bitsPerValue must be between 1 and 32.");
        }

        // Mask to extract the lower `bitsPerValue` bits
        int mask = (1 << bitsPerValue) - 1;

        // Buffer to accumulate bits from bytes
        long buffer = 0; // Use a long to handle up to 64 bits
        int bufferLength = 0; // Number of bits currently in the buffer

        for (int i = 0; i < 128; i++) {
            // Refill the buffer if it has fewer than `bitsPerValue` bits
            while (bufferLength < bitsPerValue) {
                // Read a byte from the input and add it to the buffer
                byte nextByte = pdu.in.readByte();
                buffer = (buffer << 8) | (nextByte & 0xFF); // Add 8 bits to the buffer
                bufferLength += 8;
            }

            // Extract the top `bitsPerValue` bits from the buffer
            int packedValue = (int) (buffer >>> (bufferLength - bitsPerValue)) & mask;
            ints[i] = packedValue;

            // Remove the extracted bits from the buffer
            bufferLength -= bitsPerValue;
            buffer &= (1L << bufferLength) - 1; // Clear the top bits
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
    public static byte[] bitPack(List<Integer> values, int bits) {
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
    public static List<Integer> bitUnpack(byte[] bytes, int bits) {
        if (bits == 0)
            return new ArrayList<>();

        if (bits < 1 || bits > 64) {
            throw new IllegalArgumentException("Bits must be between 1 and 64.");
        }

        List<Integer> values = new ArrayList<>();
        int bitIndex = 0; // Tracks the current bit position in the byte array

        while (bitIndex + bits <= bytes.length * 8) {
            int value = 0;
            for (int i = 0; i < bits; i++) {
                // Read the bit and add it to the value
                value |= readBit(bytes, bitIndex) << i;
                bitIndex++;
            }
            values.add(value);
        }

        return values;
    }
}
