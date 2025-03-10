package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
            if (exceptions.get(i).size() == 0)
            {
           //     out.writeVInt(0);
           //     continue;
            }

           // out.writeVInt(exceptions.get(i).size());
            byte[] bytes = LimitTestCompressor.bitPack(exceptions.get(i), i);
            out.writeVInt(bytes.length);

            out.writeBytes(bytes, bytes.length);

        }
    }

    public static HashMap<Integer, ArrayList<Integer>> decodeExceptions(IndexInput input) throws IOException {
        HashMap<Integer, ArrayList<Integer>> exceptions = new HashMap<Integer, ArrayList<Integer>>();

        for (int i = 1; i < 33; i++) {
         //   int integerCountForBitWidth = input.readVInt();
            int byteCount = input.readVInt();

            if (byteCount == 0)
                continue;

            byte[] bytes = new byte[byteCount];
            input.readBytes(bytes, 0, byteCount);
            ArrayList<Integer> ints = (ArrayList<Integer>) LimitTestCompressor.bitUnpack(bytes, i);
            exceptions.put(i, ints);

         //   for (int j = 0; j < integerCountForBitWidth; j++) {
         //       exceptions.get(i).add(input.readVInt());
         //   }
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

    public static int getLeftBits(int x, int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("Bit width must be between 0 and 32");
        }

        // Calculate the mask
        int mask = (1 << (32 - bitWidth)) - 1;

        // Shift x right by bitWidth and apply the mask
        int leftBits = (x >>> bitWidth) & mask;

        return leftBits;
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
