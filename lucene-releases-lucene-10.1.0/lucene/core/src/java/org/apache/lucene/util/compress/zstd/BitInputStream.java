/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.SIZE_OF_LONG;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.highestBit;
import static org.apache.lucene.util.compress.zstd.ZSTDUtil.verify;

/**
 * Bit streams are encoded as a byte-aligned little-endian stream. Thus, bits are laid out
 * in the following manner, and the stream is read from right to left.
 * ... [16 17 18 19 20 21 22 23] [8 9 10 11 12 13 14 15] [0 1 2 3 4 5 6 7]
 */
public final class BitInputStream
{
    private BitInputStream()
    {
    }

    public static boolean isEndOfStream(long startAddress, long currentAddress, int bitsConsumed)
    {
        return startAddress == currentAddress && bitsConsumed == Long.SIZE;
    }

    @SuppressWarnings("fallthrough")
    private static long readTail(ByteBuffer inputBase, long inputAddress, int inputSize)
    {
        long bits = inputBase.get((int)inputAddress) & 0xFF;

        switch (inputSize) {
            case 7:
                bits |= (inputBase.get((int)inputAddress + 6) & 0xFFL) << 48;
            case 6:
                bits |= (inputBase.get((int)inputAddress + 5) & 0xFFL) << 40;
            case 5:
                bits |= (inputBase.get((int)inputAddress + 4) & 0xFFL) << 32;
            case 4:
                bits |= (inputBase.get((int)inputAddress + 3) & 0xFFL) << 24;
            case 3:
                bits |= (inputBase.get((int)inputAddress + 2) & 0xFFL) << 16;
            case 2:
                bits |= (inputBase.get((int)inputAddress + 1) & 0xFFL) << 8;
        }

        return bits;
    }

    /**
     * @return numberOfBits in the low-order bits of a long
     */
    public static long peekBits(int bitsConsumed, long bitContainer, int numberOfBits)
    {
        return (((bitContainer << bitsConsumed) >>> 1) >>> (63 - numberOfBits));
    }

    /**
     * numberOfBits must be > 0
     *
     * @return numberOfBits in the low-order bits of a long
     */
    public static long peekBitsFast(int bitsConsumed, long bitContainer, int numberOfBits)
    {
        return ((bitContainer << bitsConsumed) >>> (64 - numberOfBits));
    }

    static class Initializer
    {
        private final ByteBuffer inputBase;
        private final long startAddress;
        private final long endAddress;
        private long bits;
        private long currentAddress;
        private int bitsConsumed;

        public Initializer(byte[] inputBase, long startAddress, long endAddress) throws IOException {
            this.inputBase = ByteBuffer.wrap(inputBase);
            this.startAddress = startAddress;
            this.endAddress = endAddress;
        }

        public long getBits()
        {
            return bits;
        }

        public long getCurrentAddress()
        {
            return currentAddress;
        }

        public int getBitsConsumed()
        {
            return bitsConsumed;
        }

        public void initialize()
        {
            verify(endAddress - startAddress >= 1, startAddress, "Bitstream is empty");

            int lastByte = inputBase.get((int)endAddress-1) & 0xFF;
            verify(lastByte != 0, endAddress, "Bitstream end mark not present");

            bitsConsumed = SIZE_OF_LONG - highestBit(lastByte);

            int inputSize = (int) (endAddress - startAddress);
            if (inputSize >= SIZE_OF_LONG) {  /* normal case */
                currentAddress = endAddress - SIZE_OF_LONG;
                bits = inputBase.getLong((int)currentAddress);
            }
            else {
                currentAddress = startAddress;
                bits = readTail(inputBase, startAddress, inputSize);

                bitsConsumed += (SIZE_OF_LONG - inputSize) * 8;
            }
        }
    }

    static final class Loader
    {
        private final ByteBuffer inputBase;
        private final long startAddress;
        private long bits;
        private long currentAddress;
        private int bitsConsumed;
        private boolean overflow;

        public Loader(byte[] inputBase, long startAddress, long currentAddress, long bits, int bitsConsumed) throws IOException {
            this.inputBase = ByteBuffer.wrap(inputBase);
            this.startAddress = startAddress;
            this.bits = bits;
            this.currentAddress = currentAddress;
            this.bitsConsumed = bitsConsumed;
        }

        public long getBits()
        {
            return bits;
        }

        public long getCurrentAddress()
        {
            return currentAddress;
        }

        public int getBitsConsumed()
        {
            return bitsConsumed;
        }

        public boolean isOverflow()
        {
            return overflow;
        }

        public boolean load()
        {
            if (bitsConsumed > 64) {
                overflow = true;
                return true;
            }

            else if (currentAddress == startAddress) {
                return true;
            }

            int bytes = bitsConsumed >>> 3; // divide by 8
            if (currentAddress >= startAddress + SIZE_OF_LONG) {
                if (bytes > 0) {
                    currentAddress -= bytes;
                    bits = inputBase.getLong((int)currentAddress);
                }
                bitsConsumed &= 0b111;
            }
            else if (currentAddress - bytes < startAddress) {
                bytes = (int) (currentAddress - startAddress);
                currentAddress = startAddress;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = inputBase.getLong((int)startAddress);
                return true;
            }
            else {
                currentAddress -= bytes;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = inputBase.getLong((int)currentAddress);
            }

            return false;
        }
    }
}
