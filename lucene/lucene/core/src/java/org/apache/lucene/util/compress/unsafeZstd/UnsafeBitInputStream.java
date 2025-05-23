/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

import static org.apache.lucene.util.compress.unsafeZstd.UnsafeConstants.SIZE_OF_LONG;
import static org.apache.lucene.util.UnsafeUtil.UNSAFE;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.highestBit;
import static org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTDUtil.verify;

/**
 * Bit streams are encoded as a byte-aligned little-endian stream. Thus, bits are laid out
 * in the following manner, and the stream is read from right to left.
 * <p>
 * <p>
 * ... [16 17 18 19 20 21 22 23] [8 9 10 11 12 13 14 15] [0 1 2 3 4 5 6 7]
 */
final class UnsafeBitInputStream
{
    private UnsafeBitInputStream()
    {
    }

    public static boolean isEndOfStream(long startAddress, long currentAddress, int bitsConsumed)
    {
        return startAddress == currentAddress && bitsConsumed == Long.SIZE;
    }

    private static long readTail(Object inputBase, long inputAddress, int inputSize)
    {
        long bits = UNSAFE.getByte(inputBase, inputAddress) & 0xFF;

        switch (inputSize) {
            case 7:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 6) & 0xFFL) << 48;
            case 6:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 5) & 0xFFL) << 40;
            case 5:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 4) & 0xFFL) << 32;
            case 4:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 3) & 0xFFL) << 24;
            case 3:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 2) & 0xFFL) << 16;
            case 2:
                bits |= (UNSAFE.getByte(inputBase, inputAddress + 1) & 0xFFL) << 8;
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
        private final Object inputBase;
        private final long startAddress;
        private final long endAddress;
        private long bits;
        private long currentAddress;
        private int bitsConsumed;

        public Initializer(Object inputBase, long startAddress, long endAddress)
        {
            this.inputBase = inputBase;
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

            int lastByte = UNSAFE.getByte(inputBase, endAddress - 1) & 0xFF;
            verify(lastByte != 0, endAddress, "Bitstream end mark not present");

            bitsConsumed = SIZE_OF_LONG - highestBit(lastByte);

            int inputSize = (int) (endAddress - startAddress);
            if (inputSize >= SIZE_OF_LONG) {  /* normal case */
                currentAddress = endAddress - SIZE_OF_LONG;
                bits = UNSAFE.getLong(inputBase, currentAddress);
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
        private final Object inputBase;
        private final long startAddress;
        private long bits;
        private long currentAddress;
        private int bitsConsumed;
        private boolean overflow;

        public Loader(Object inputBase, long startAddress, long currentAddress, long bits, int bitsConsumed)
        {
            this.inputBase = inputBase;
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
                    bits = UNSAFE.getLong(inputBase, currentAddress);
                }
                bitsConsumed &= 0b111;
            }
            else if (currentAddress - bytes < startAddress) {
                bytes = (int) (currentAddress - startAddress);
                currentAddress = startAddress;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = UNSAFE.getLong(inputBase, startAddress);
                return true;
            }
            else {
                currentAddress -= bytes;
                bitsConsumed -= bytes * SIZE_OF_LONG;
                bits = UNSAFE.getLong(inputBase, currentAddress);
            }

            return false;
        }
    }
}