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
        int execute(int bitWidth, int totalExceptions, int c);
    }

    // Find the optimal bit width for the regular values
//    public static int getBestBitWidth(CostFunction costFunction) {
     //   costFunction.execute()
  //  }

    public static void getMinMaxValue(int[] ints, Integer min, Integer max)
    {
        min = ints[0];
        max = ints[0];
        for (int i = 0; i < 128; i++) {
            if (min > ints[i])
                min = ints[i];
            if (max < ints[i])
                max = ints[i];
        }
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
            byte[] bytes = LimitTestCompressor.bitPack(exceptions.get(i), i);
            out.writeVInt(bytes.length);

            out.writeBytes(bytes, bytes.length);
        }
    }

    public static HashMap<Integer, ArrayList<Integer>> decodeExceptions(IndexInput input) throws IOException {
        HashMap<Integer, ArrayList<Integer>> exceptions = new HashMap<Integer, ArrayList<Integer>>();

        for (int i = 1; i < 33; i++) {
            int byteCount = input.readVInt();

            if (byteCount == 0)
                continue;

            byte[] bytes = new byte[byteCount];
            input.readBytes(bytes, 0, byteCount);
            ArrayList<Integer> ints = (ArrayList<Integer>) LimitTestCompressor.bitUnpack(bytes, i);
            exceptions.put(i, ints);
        }
        return exceptions;
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
