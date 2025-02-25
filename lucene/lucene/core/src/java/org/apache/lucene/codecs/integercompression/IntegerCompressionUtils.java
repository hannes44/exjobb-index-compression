package org.apache.lucene.codecs.integercompression;

public class IntegerCompressionUtils {
    public static long getMaxValue(long[] longs)
    {
        long maxValue = longs[0];
        for (int i = 0; i < 128; i++) {
            if (maxValue < longs[i])
                maxValue = longs[i];
        }
        return maxValue;
    }

    public static long getMinValue(long[] longs)
    {
        long minValue = longs[0];
        for (int i = 0; i < 128; i++) {
            if (minValue > longs[i])
                minValue = longs[i];
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
}
