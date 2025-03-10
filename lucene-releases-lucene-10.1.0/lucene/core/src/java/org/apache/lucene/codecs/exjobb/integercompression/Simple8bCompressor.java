package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implements simple8b compression for 128 integer blocks
 */
public class Simple8bCompressor implements IntegerCompressor {

    // Number of values that can be stored per 64-bit word for each selector mode
    public static final int[] NUM_VALUES = {
            60, // Mode 0: 60 values, 1-bit each
            30, // Mode 1: 30 values, 2-bit each
            20, // Mode 2: 20 values, 3-bit each
            15, // Mode 3: 15 values, 4-bit each
            12, // Mode 4: 12 values, 5-bit each
            10, // Mode 5: 10 values, 6-bit each
            8,  // Mode 6: 8 values, 7-bit each
            7,  // Mode 7: 7 values, 8-bit each
            6,  // Mode 8: 6 values, 10-bit each
            5,  // Mode 9: 5 values, 12-bit each
            4,  // Mode 10: 4 values, 15-bit each
            3,  // Mode 11: 3 values, 20-bit each
            2,  // Mode 12: 2 values, 30-bit each
            1   // Mode 13: 1 value, 60-bit each
    };

    // Corresponding bit widths for each selector mode
    public static final int[] BIT_WIDTHS = {
            1,  // Mode 0: 1-bit values
            2,  // Mode 1: 2-bit values
            3,  // Mode 2: 3-bit values
            4,  // Mode 3: 4-bit values
            5,  // Mode 4: 5-bit values
            6,  // Mode 5: 6-bit values
            7,  // Mode 6: 7-bit values
            8,  // Mode 7: 8-bit values
            10, // Mode 8: 10-bit values
            12, // Mode 9: 12-bit values
            15, // Mode 10: 15-bit values
            20, // Mode 11: 20-bit values
            30, // Mode 12: 30-bit values
            60  // Mode 13: 60-bit values
    };

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        int i = 0;
        while (i < 128) {
            long encodedWord = 0;
            int selector = -1;
            int numValues = 0;

            // Find the best selector that fits the next values
            for (int mode = 0; mode < NUM_VALUES.length; mode++) {
                int bitWidth = BIT_WIDTHS[mode];
                int maxValues = NUM_VALUES[mode];

                if (i + maxValues > 128) continue; // Don't overflow array

                boolean fits = true;
                for (int j = 0; j < maxValues; j++) {
                    if ((ints[i + j] >>> bitWidth) != 0) {
                        fits = false;
                        break;
                    }
                }

                if (fits) {
                    selector = mode;
                    numValues = maxValues;
                    break; // Found a suitable mode
                }
            }

            if (selector == -1) {
                throw new IOException("Value too large for Simple-8b encoding");
            }

            // Encode values
            encodedWord |= (long) selector << 60; // Store selector in highest 4 bits
            for (int j = 0; j < numValues; j++) {
                encodedWord |= ((long) ints[i + j] & ((1L << BIT_WIDTHS[selector]) - 1))
                        << (j * BIT_WIDTHS[selector]);
            }

            out.writeLong(encodedWord); // Store compressed word
            i += numValues;
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
        int i = 0;

        while (i < 128) {
            long encodedWord = pdu.in.readLong(); // Read a compressed 64-bit word
            int selector = (int) (encodedWord >>> 60); // Extract selector (highest 4 bits)
            int numValues = NUM_VALUES[selector]; // Number of integers encoded
            int bitWidth = BIT_WIDTHS[selector]; // Bits per integer

            // Decode each integer
            for (int j = 0; j < numValues && i < 128; j++, i++) {
                ints[i] = (int) ((encodedWord >>> (j * bitWidth)) & ((1L << bitWidth) - 1));
            }
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.NONE;
    }


}
