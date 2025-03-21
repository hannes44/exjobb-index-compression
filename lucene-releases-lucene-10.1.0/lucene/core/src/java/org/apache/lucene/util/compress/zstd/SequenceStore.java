/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_LONG;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_LITERALS_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_MATCH_LENGTH_SYMBOL;
import static org.apache.lucene.util.compress.zstd.ZSTD.readLong;
import static org.apache.lucene.util.compress.zstd.ZSTD.writeLong;

final class SequenceStore
{
    public final byte[] literalsBuffer;
    public int literalsLength;

    public final int[] offsets;
    public final int[] literalLengths;
    public final int[] matchLengths;
    public int sequenceCount;

    public final byte[] literalLengthCodes;
    public final byte[] matchLengthCodes;
    public final byte[] offsetCodes;

    public LongField longLengthField;
    public int longLengthPosition;

    public enum LongField
    {
        LITERAL, MATCH
    }

    private static final byte[] LITERAL_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 10, 11, 12, 13, 14, 15,
            16, 16, 17, 17, 18, 18, 19, 19,
            20, 20, 20, 20, 21, 21, 21, 21,
            22, 22, 22, 22, 22, 22, 22, 22,
            23, 23, 23, 23, 23, 23, 23, 23,
            24, 24, 24, 24, 24, 24, 24, 24,
            24, 24, 24, 24, 24, 24, 24, 24};

    private static final byte[] MATCH_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
            38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
            40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
            41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
            42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
            42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42};

    public SequenceStore(int blockSize, int maxSequences)
    {
        offsets = new int[maxSequences];
        literalLengths = new int[maxSequences];
        matchLengths = new int[maxSequences];

        literalLengthCodes = new byte[maxSequences];
        matchLengthCodes = new byte[maxSequences];
        offsetCodes = new byte[maxSequences];

        literalsBuffer = new byte[blockSize];

        reset();
    }

    public void appendLiterals(byte[] inputBase, long inputOffset, int inputSize)
    {
        System.arraycopy(inputBase, (int) inputOffset, literalsBuffer, literalsLength, inputSize);
        literalsLength += inputSize;
    }

    public void storeSequence(byte[] literalBase, long literalAddress, int literalLength, int offsetCode, int matchLengthBase)
    {
        long input = literalAddress;
        long output = literalsLength;
        int copied = 0;
        do {
            writeLong(literalsBuffer, output, readLong(literalBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
            copied += SIZE_OF_LONG;
        }
        while (copied < literalLength);

        literalsLength += literalLength;

        if (literalLength > 65535) {
            longLengthField = LongField.LITERAL;
            longLengthPosition = sequenceCount;
        }
        literalLengths[sequenceCount] = literalLength;

        offsets[sequenceCount] = offsetCode + 1;

        if (matchLengthBase > 65535) {
            longLengthField = LongField.MATCH;
            longLengthPosition = sequenceCount;
        }

        matchLengths[sequenceCount] = matchLengthBase;

        sequenceCount++;
    }

    public void reset()
    {
        literalsLength = 0;
        sequenceCount = 0;
        longLengthField = null;
    }

    public void generateCodes()
    {
        for (int i = 0; i < sequenceCount; ++i) {
            literalLengthCodes[i] = (byte) literalLengthToCode(literalLengths[i]);
            offsetCodes[i] = (byte) ZSTDUtil.highestBit(offsets[i]);
            matchLengthCodes[i] = (byte) matchLengthToCode(matchLengths[i]);
        }

        if (longLengthField == LongField.LITERAL) {
            literalLengthCodes[longLengthPosition] = MAX_LITERALS_LENGTH_SYMBOL;
        }
        if (longLengthField == LongField.MATCH) {
            matchLengthCodes[longLengthPosition] = MAX_MATCH_LENGTH_SYMBOL;
        }
    }

    private static int literalLengthToCode(int literalLength)
    {
        if (literalLength >= 64) {
            return ZSTDUtil.highestBit(literalLength) + 19;
        }
        else {
            return LITERAL_LENGTH_CODE[literalLength];
        }
    }

    /*
     * matchLengthBase = matchLength - MINMATCH
     * (that's how it's stored in SequenceStore)
     */
    private static int matchLengthToCode(int matchLengthBase)
    {
        if (matchLengthBase >= 128) {
            return ZSTDUtil.highestBit(matchLengthBase) + 36;
        }
        else {
            return MATCH_LENGTH_CODE[matchLengthBase];
        }
    }
}
