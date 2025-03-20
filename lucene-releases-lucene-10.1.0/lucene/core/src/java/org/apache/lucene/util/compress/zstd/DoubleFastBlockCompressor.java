/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_INT;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_LONG;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.REPEATED_OFFSET_COUNT;
import static org.apache.lucene.util.compress.zstd.ZSTD.readInt;
import static org.apache.lucene.util.compress.zstd.ZSTD.readLong;

public class DoubleFastBlockCompressor
        implements BlockCompressor
{
    private static final int MIN_MATCH = 3;
    private static final int SEARCH_STRENGTH = 8;
    private static final int REP_MOVE = REPEATED_OFFSET_COUNT - 1;

    @Override
    public int compressBlock(byte[] inputBase, final long inputAddress, int inputSize, SequenceStore output, BlockCompressionState state, RepeatedOffsets offsets, CompressionParameters parameters)
    {
        int matchSearchLength = Math.max(parameters.getSearchLength(), 4);

        // Offsets in hash tables are relative to baseAddress. Hash tables can be reused across calls to compressBlock as long as
        // baseAddress is kept constant.
        // We don't want to generate sequences that point before the current window limit, so we "filter" out all results from looking up in the hash tables
        // beyond that point.
        final long baseAddress = state.getBaseAddress();
        final long windowBaseAddress = baseAddress + state.getWindowBaseOffset();

        int[] longHashTable = state.hashTable;
        int longHashBits = parameters.getHashLog();

        int[] shortHashTable = state.chainTable;
        int shortHashBits = parameters.getChainLog();

        final long inputEnd = inputAddress + inputSize;
        final long inputLimit = inputEnd - SIZE_OF_LONG; // We read a long at a time for computing the hashes

        long input = inputAddress;
        long anchor = inputAddress;

        int offset1 = offsets.getOffset0();
        int offset2 = offsets.getOffset1();

        int savedOffset = 0;

        if (input - windowBaseAddress == 0) {
            input++;
        }
        int maxRep = (int) (input - windowBaseAddress);

        if (offset2 > maxRep) {
            savedOffset = offset2;
            offset2 = 0;
        }

        if (offset1 > maxRep) {
            savedOffset = offset1;
            offset1 = 0;
        }

        while (input < inputLimit) {   // < instead of <=, because repcode check at (input+1)
            int shortHash = hash(inputBase, input, shortHashBits, matchSearchLength);
            long shortMatchAddress = baseAddress + shortHashTable[shortHash];

            int longHash = hash8(readLong(inputBase, input), longHashBits);
            long longMatchAddress = baseAddress + longHashTable[longHash];

            // update hash tables
            int current = (int) (input - baseAddress);
            longHashTable[longHash] = current;
            shortHashTable[shortHash] = current;

            int matchLength;
            int offset;

            if (offset1 > 0 && readInt(inputBase, (int) input + 1 - offset1) ==readInt(inputBase, input + 1)) {
                // found a repeated sequence of at least 4 bytes, separated by offset1
                matchLength = count(inputBase, input + 1 + SIZE_OF_INT, inputEnd, input + 1 + SIZE_OF_INT - offset1) + SIZE_OF_INT;
                input++;
                output.storeSequence(inputBase, anchor, (int) (input - anchor), 0, matchLength - MIN_MATCH);
            }
            else {
                // check prefix long match
                if (longMatchAddress > windowBaseAddress && readLong(inputBase, longMatchAddress) == readLong(inputBase, input)) {
                    matchLength = count(inputBase, input + SIZE_OF_LONG, inputEnd, longMatchAddress + SIZE_OF_LONG) + SIZE_OF_LONG;
                    offset = (int) (input - longMatchAddress);
                    while (input > anchor && longMatchAddress > windowBaseAddress && inputBase[(int) input-1] == inputBase[(int)longMatchAddress-1]) {
                        input--;
                        longMatchAddress--;
                        matchLength++;
                    }
                }
                else {
                    // check prefix short match
                    if (shortMatchAddress > windowBaseAddress && readInt(inputBase, longMatchAddress) == readInt(inputBase, input)) {
                        int nextOffsetHash = hash8(readLong(inputBase, input+1), longHashBits);
                        long nextOffsetMatchAddress = baseAddress + longHashTable[nextOffsetHash];
                        longHashTable[nextOffsetHash] = current + 1;

                        // check prefix long +1 match
                        if (nextOffsetMatchAddress > windowBaseAddress && readLong(inputBase, nextOffsetMatchAddress) == readLong(inputBase, input+1)) {
                            matchLength = count(inputBase, input + 1 + SIZE_OF_LONG, inputEnd, nextOffsetMatchAddress + SIZE_OF_LONG) + SIZE_OF_LONG;
                            input++;
                            offset = (int) (input - nextOffsetMatchAddress);
                            while (input > anchor && nextOffsetMatchAddress > windowBaseAddress && inputBase[(int)input - 1] == inputBase[(int)nextOffsetMatchAddress - 1]) {
                                input--;
                                nextOffsetMatchAddress--;
                                matchLength++;
                            }
                        }
                        else {
                            // if no long +1 match, explore the short match we found
                            matchLength = count(inputBase, input + SIZE_OF_INT, inputEnd, shortMatchAddress + SIZE_OF_INT) + SIZE_OF_INT;
                            offset = (int) (input - shortMatchAddress);
                            while (input > anchor && shortMatchAddress > windowBaseAddress && inputBase[(int)input - 1] == inputBase[(int)shortMatchAddress - 1]) {
                                input--;
                                shortMatchAddress--;
                                matchLength++;
                            }
                        }
                    }
                    else {
                        input += ((input - anchor) >> SEARCH_STRENGTH) + 1;
                        continue;
                    }
                }

                offset2 = offset1;
                offset1 = offset;

                output.storeSequence(inputBase, anchor, (int) (input - anchor), offset + REP_MOVE, matchLength - MIN_MATCH);
            }

            input += matchLength;
            anchor = input;

            if (input <= inputLimit) { // TODO WORK FROM HERE (DO WHOLE FILE)
                // Fill Table
                longHashTable[hash8(readLong(inputBase, baseAddress + current + 2), longHashBits)] = current + 2;
                shortHashTable[hash(inputBase, baseAddress + current + 2, shortHashBits, matchSearchLength)] = current + 2;

                longHashTable[hash8(readLong(inputBase, input - 2), longHashBits)] = (int) (input - 2 - baseAddress);
                shortHashTable[hash(inputBase, input - 2, shortHashBits, matchSearchLength)] = (int) (input - 2 - baseAddress);

                while (input <= inputLimit && offset2 > 0 && readInt(inputBase, input) == readInt(inputBase, input - offset2)) {
                    int repetitionLength = count(inputBase, input + SIZE_OF_INT, inputEnd, input + SIZE_OF_INT - offset2) + SIZE_OF_INT;

                    // swap offset2 <=> offset1
                    int temp = offset2;
                    offset2 = offset1;
                    offset1 = temp;

                    shortHashTable[hash(inputBase, input, shortHashBits, matchSearchLength)] = (int) (input - baseAddress);
                    longHashTable[hash8(readLong(inputBase, input), longHashBits)] = (int) (input - baseAddress);

                    output.storeSequence(inputBase, anchor, 0, 0, repetitionLength - MIN_MATCH);

                    input += repetitionLength;
                    anchor = input;
                }
            }
        }

        // save reps for next block
        offsets.saveOffset0(offset1 != 0 ? offset1 : savedOffset);
        offsets.saveOffset1(offset2 != 0 ? offset2 : savedOffset);

        // return the last literals size
        return (int) (inputEnd - anchor);
    }

    // TODO: same as LZ4RawCompressor.count

    /**
     {@code matchAddress} must be {@code <} {@code inputAddress}
     */
    public static int count(byte[] inputBase, final long inputAddress, final long inputLimit, final long matchAddress)
    {
        long input = inputAddress;
        long match = matchAddress;

        int remaining = (int) (inputLimit - inputAddress);

        // first, compare long at a time
        int count = 0;
        while (count < remaining - (SIZE_OF_LONG - 1)) {
            long diff = readLong(inputBase, match) ^ readLong(inputBase, input);
            if (diff != 0) {
                return count + (Long.numberOfTrailingZeros(diff) >> 3);
            }

            count += SIZE_OF_LONG;
            input += SIZE_OF_LONG;
            match += SIZE_OF_LONG;
        }

        while (count < remaining && inputBase[(int) match] == inputBase[(int) input]) {
            count++;
            input++;
            match++;
        }

        return count;
    }

    private static int hash(byte[] inputBase, long inputAddress, int bits, int matchSearchLength)
    {
        switch (matchSearchLength) {
            case 8:
                return hash8(readLong(inputBase, inputAddress), bits);
            case 7:
                return hash7(readLong(inputBase, inputAddress), bits);
            case 6:
                return hash6(readLong(inputBase, inputAddress), bits);
            case 5:
                return hash5(readLong(inputBase, inputAddress), bits);
            default:
                return hash4(readInt(inputBase, inputAddress), bits);
        }
    }

    private static final int PRIME_4_BYTES = 0x9E3779B1;
    private static final long PRIME_5_BYTES = 0xCF1BBCDCBBL;
    private static final long PRIME_6_BYTES = 0xCF1BBCDCBF9BL;
    private static final long PRIME_7_BYTES = 0xCF1BBCDCBFA563L;
    private static final long PRIME_8_BYTES = 0xCF1BBCDCB7A56463L;

    private static int hash4(int value, int bits)
    {
        return (value * PRIME_4_BYTES) >>> (Integer.SIZE - bits);
    }

    private static int hash5(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 40)) * PRIME_5_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash6(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 48)) * PRIME_6_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash7(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 56)) * PRIME_7_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash8(long value, int bits)
    {
        return (int) ((value * PRIME_8_BYTES) >>> (Long.SIZE - bits));
    }
}
