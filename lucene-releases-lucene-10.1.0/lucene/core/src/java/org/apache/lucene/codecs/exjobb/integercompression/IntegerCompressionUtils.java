package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

/** Utils for integer compression */
public class IntegerCompressionUtils {
    public static int getMaxValue(int[] ints)
    {
        int maxValue = ints[0];
        for (int i = 0; i < 128; i++) {
            if (maxValue < ints[i])
                maxValue = ints[i];
        }
        return maxValue;
    }

    public static int getMinValue(int[] ints)
    {
        int minValue = ints[0];
        for (int i = 0; i < 128; i++) {
            if (minValue > ints[i])
                minValue = ints[i];
        }
        return minValue;
    }

    public static boolean isBitMaskZero(byte[] bitMask) {
        for (int i = 0; i < bitMask.length; i++) {
            if (bitMask[i] != 0)
                return false;
        }
        return true;
    }

    // Will map bit count to the ints that require x bits
    public static HashMap<Integer, List<Integer>> getBitCountToIndexMap(int[] ints) {
        HashMap<Integer, List<Integer>> bitsNeededCount = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            int bitsRequired = PackedInts.bitsRequired(ints[i]);
            if (!bitsNeededCount.containsKey(bitsRequired)) {
                bitsNeededCount.put(bitsRequired, new ArrayList<>());
            }
            bitsNeededCount.get(bitsRequired).add(i);
        }
        return bitsNeededCount;
    }

    @FunctionalInterface
    interface CostFunction {
        int execute(int bitWidth, int totalExceptions, int maxBitsRequired);
    }

    // Find the optimal bit width for the regular values
    public static int getBestBitWidth(CostFunction costFunction, int maxBitsRequired, HashMap<Integer, List<Integer>> bitCountToIndex) {


        int totalExceptions = 0;
        int minBitsRequired = maxBitsRequired * 128;
        int bestBitWidth = maxBitsRequired;
        for (int i = 32; i > 0; i--) {
            if (bitCountToIndex.containsKey(i)) {
                int bitsRequired = costFunction.execute(i, totalExceptions, maxBitsRequired);

                if (minBitsRequired > bitsRequired)
                {
                    minBitsRequired = bitsRequired;
                    bestBitWidth = i;
                }
                totalExceptions += bitCountToIndex.get(i).size();
            }
        }

        return bestBitWidth;
    }



    public static void setupExceptionHashmap(HashMap<Integer, ArrayList<Integer>> exceptions)
    {
        for (int i = 0; i < 33; i++)
        {
            exceptions.put(i, new ArrayList<Integer>());
        }
    }

    public static void encodeExceptions(HashMap<Integer, ArrayList<Integer>> exceptions, DataOutput out) throws IOException {
        for (int i = 1; i < 33; i++) {
            //           byte[] bytes = IntegerCompressionUtils.bitPack(exceptions.get(i), i);
            //out.writeVInt(bytes.length);

//            out.writeBytes(bytes, bytes.length);
        }
    }

    public static void getExceptionsFromIndexMap(List<Integer> exceptionIndices, List<Integer> exceptionValues, HashMap<Integer, List<Integer>> bitCountToIndex, int[] ints, int bestBitWidth) {
        for (int i = 32; i > 0; i--) {
            if (bitCountToIndex.containsKey(i))
            {
                if (i > bestBitWidth) {
                    for (Integer index : bitCountToIndex.get(i))
                    {
                        exceptionIndices.add(index);
                        exceptionValues.add(ints[index]);
                    }
                }
                else {
                    break;
                }
            }
        }
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


    public static HashMap<Integer, ArrayList<Integer>> decodeExceptions(IndexInput input) throws IOException {
        HashMap<Integer, ArrayList<Integer>> exceptions = new HashMap<Integer, ArrayList<Integer>>();

        for (int i = 1; i < 33; i++) {

       //     int byteCount = input.readVInt();

       //     if (byteCount == 0)
       //         continue;

       //     byte[] bytes = new byte[byteCount];
       //     input.readBytes(bytes, 0, byteCount);
        //    ArrayList<Integer> ints = (ArrayList<Integer>) LimitTestCompressor.bitUnpack(bytes, i);
     //       exceptions.put(i, ints);
        }
        return exceptions;
    }

    public static MinMax findMinMax(int[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("Array must not be null or empty");
        }

        int min = arr[0], max = arr[0];
        for (int i = 1; i < arr.length; i++) {
            int val = arr[i];
            if (val < min) min = val;
            if (val > max) max = val;
        }

        return new MinMax(min, max);
    }

    public static void setNthBit(byte[] byteArray, int n) {
        // Calculate the byte index and bit position
        int byteIndex = n / 8;       // Which byte contains the nth bit
        int bitPosition = n % 8;     // Which bit in the byte to set

        // Set the nth bit to 1 using bitwise OR
        byteArray[byteIndex] |= (1 << bitPosition);
    }

    public static int getNthBit(byte[] byteArray, int n) {
        // Calculate the byte index and bit position
        int byteIndex = n / 8;       // Which byte contains the nth bit
        int bitPosition = n % 8;     // Which bit in the byte to get

        // Extract the nth bit using bitwise AND and right shift
        return (byteArray[byteIndex] >> bitPosition) & 1;
    }

    public static int getLeftBits(int x, int leftBitCount) {
        if (leftBitCount < 0 || leftBitCount > 32) {
            throw new IllegalArgumentException("leftBitCount must be between 0 and 32.");
        }

        if (leftBitCount == 0) {
            return 0; // No bits to retain
        }

        // Shift the leftmost `leftBitCount` bits to the rightmost positions
        int shifted = x >>> (32 - leftBitCount);

        // Mask to retain only the leftmost `leftBitCount` bits
        int mask = (1 << leftBitCount) - 1;
        return shifted & mask;
    }

    public static void turnDeltasIntoAbsolutes(int[] deltas)
    {
        if (deltas == null || deltas.length == 0) {
            return;
        }

        // The first value is already an absolute value
        for (int i = 1; i < deltas.length; i++) {
            deltas[i] += deltas[i - 1]; // Convert delta to absolute value
        }
    }

    public static void turnAbsolutesIntoDeltas(int[] absolutes) {
        if (absolutes == null || absolutes.length == 0) {
            return; // Handle empty or null input
        }

        // Start from the end and work backwards to avoid overwriting values
        for (int i = absolutes.length - 1; i > 0; i--) {
            absolutes[i] -= absolutes[i - 1]; // Convert absolute value to delta
        }
    }

    /** Analysis the given ints and returns the best compression technique */
    public static void findOptimalCompressionTechniqueForInts(int[] ints)
    {

    }

    public static int optimalNumberOfBitsRequiredForFORCompression(int[] ints)
    {
        int maxValue = getMaxValue(ints);
        int minValue = getMinValue(ints);

        return 1;
    }

    public static int optimalNumberOfBitsRequiredForPFORCompression(int[] ints)
    {
        return 1;
    }

    public static int optimalNumberOfBitsRequiredForFASTPFORCompression(int[] ints)
    {
        return 1;
    }

    public static int optimalNumberOfBitsRequiredForSimple8BCompression(int[] ints)
    {
        return 1;
    }

}
