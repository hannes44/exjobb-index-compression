/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import org.apache.lucene.store.DataInput;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.util.compress.zstd.FiniteStateEntropy.MAX_SYMBOL;
import static org.apache.lucene.util.compress.zstd.FiniteStateEntropy.MIN_TABLE_LOG;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.highestBit;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.verify;

public class FseTableReader
{
    private final short[] nextSymbol = new short[MAX_SYMBOL + 1];
    private final short[] normalizedCounters = new short[MAX_SYMBOL + 1];

    public int readFseTable(FiniteStateEntropy.Table table, DataInput inputBase, long inputAddress, long inputLimit, int maxSymbol, int maxTableLog, byte[] originalInput) throws IOException {
        // read table headers
        long input = inputAddress;
        verify(inputLimit - inputAddress >= 4, input, "Not enough input bytes");

        int threshold;
        int symbolNumber = 0;
        boolean previousIsZero = false;

        ByteBuffer inputBuffer = ByteBuffer.wrap(originalInput);
        int bitStream = inputBuffer.getInt((int)input);

        int tableLog = (bitStream & 0xF) + MIN_TABLE_LOG;

        int numberOfBits = tableLog + 1;
        bitStream >>>= 4;
        int bitCount = 4;

        verify(tableLog <= maxTableLog, input, "FSE table size exceeds maximum allowed size");

        int remaining = (1 << tableLog) + 1;
        threshold = 1 << tableLog;

        while (remaining > 1 && symbolNumber <= maxSymbol) {
            if (previousIsZero) {
                int n0 = symbolNumber;
                while ((bitStream & 0xFFFF) == 0xFFFF) {
                    n0 += 24;
                    if (input < inputLimit - 5) {
                        input += 2;
                        bitStream = (inputBuffer.getInt((int)input) >>> bitCount);
                    }
                    else {
                        // end of bit stream
                        bitStream >>>= 16;
                        bitCount += 16;
                    }
                }
                while ((bitStream & 3) == 3) {
                    n0 += 3;
                    bitStream >>>= 2;
                    bitCount += 2;
                }
                n0 += bitStream & 3;
                bitCount += 2;

                verify(n0 <= maxSymbol, input, "Symbol larger than max value");

                while (symbolNumber < n0) {
                    normalizedCounters[symbolNumber++] = 0;
                }
                if ((input <= inputLimit - 7) || (input + (bitCount >>> 3) <= inputLimit - 4)) {
                    input += bitCount >>> 3;
                    bitCount &= 7;
                    bitStream = inputBuffer.getInt((int)input) >>> bitCount;
                }
                else {
                    bitStream >>>= 2;
                }
            }

            short max = (short) ((2 * threshold - 1) - remaining);
            short count;

            if ((bitStream & (threshold - 1)) < max) {
                count = (short) (bitStream & (threshold - 1));
                bitCount += numberOfBits - 1;
            }
            else {
                count = (short) (bitStream & (2 * threshold - 1));
                if (count >= threshold) {
                    count -= max;
                }
                bitCount += numberOfBits;
            }
            count--;  // extra accuracy

            remaining -= Math.abs(count);
            normalizedCounters[symbolNumber++] = count;
            previousIsZero = count == 0;
            while (remaining < threshold) {
                numberOfBits--;
                threshold >>>= 1;
            }

            if ((input <= inputLimit - 7) || (input + (bitCount >> 3) <= inputLimit - 4)) {
                input += bitCount >>> 3;
                bitCount &= 7;
            }
            else {
                bitCount -= (int) (8 * (inputLimit - 4 - input));
                input = (int) inputLimit - 4;
            }
            bitStream = inputBuffer.getInt((int)input) >>> (bitCount & 31);
        }

        verify(remaining == 1 && bitCount <= 32, input, "Input is corrupted");

        maxSymbol = symbolNumber - 1;
        verify(maxSymbol <= MAX_SYMBOL, input, "Max symbol value too large (too many symbols for FSE)");

        input += (bitCount + 7) >> 3;
        // Move file pointer to the next byte boundary
        inputBase.skipBytes((bitCount + 7) >> 3);

        // populate decoding table
        int symbolCount = maxSymbol + 1;
        int tableSize = 1 << tableLog;
        int highThreshold = tableSize - 1;

        table.log2Size = tableLog;

        for (byte symbol = 0; symbol < symbolCount; symbol++) {
            if (normalizedCounters[symbol] == -1) {
                table.symbol[highThreshold--] = symbol;
                nextSymbol[symbol] = 1;
            }
            else {
                nextSymbol[symbol] = normalizedCounters[symbol];
            }
        }

        int position = FseCompressionTable.spreadSymbols(normalizedCounters, maxSymbol, tableSize, highThreshold, table.symbol);

        // position must reach all cells once, otherwise normalizedCounter is incorrect
        verify(position == 0, input, "Input is corrupted");

        for (int i = 0; i < tableSize; i++) {
            byte symbol = table.symbol[i];
            short nextState = nextSymbol[symbol]++;
            table.numberOfBits[i] = (byte) (tableLog - highestBit(nextState));
            table.newState[i] = (short) ((nextState << table.numberOfBits[i]) - tableSize);
        }

        return (int) (input - inputAddress);
    }

    public static void initializeRleTable(FiniteStateEntropy.Table table, byte value)
    {
        table.log2Size = 0;
        table.symbol[0] = value;
        table.newState[0] = 0;
        table.numberOfBits[0] = 0;
    }
}
