/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import org.apache.lucene.util.compress.zstd.IncompatibleJvmException;
import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;

import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

final class UnsafeUtil
{
    public static final Unsafe UNSAFE;

    private UnsafeUtil() {}

    static {
        ByteOrder order = ByteOrder.nativeOrder();
        if (!order.equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IncompatibleJvmException(format("Zstandard requires a little endian platform (found %s)", order));
        }

        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        }
        catch (Exception e) {
            throw new IncompatibleJvmException("Zstandard requires access to sun.misc.Unsafe");
        }
    }

    public static byte[] getBase(MemorySegment segment)
    {
        if (segment.isNative()) {
            return null;
        }
        if (segment.isReadOnly()) {
            throw new IllegalArgumentException("MemorySegment is read-only");
        }
        Object inputBase = segment.heapBase().orElse(null);
        if (!(inputBase instanceof byte[] byteArray)) {
            throw new IllegalArgumentException("MemorySegment is not backed by a byte array");
        }
        return byteArray;
    }

    public static long getAddress(MemorySegment segment)
    {
        if (segment.isNative()) {
            return segment.address();
        }
        return segment.address() + ARRAY_BYTE_BASE_OFFSET;
    }
}
