package org.apache.lucene.codecs.exjobb.integercompression;

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
