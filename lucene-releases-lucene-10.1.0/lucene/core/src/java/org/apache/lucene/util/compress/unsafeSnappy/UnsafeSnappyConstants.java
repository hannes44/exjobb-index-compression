/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeSnappy;

public final class UnsafeSnappyConstants
{
    static final int SIZE_OF_SHORT = 2;
    static final int SIZE_OF_INT = 4;
    static final int SIZE_OF_LONG = 8;

    static final int LITERAL = 0;
    static final int COPY_1_BYTE_OFFSET = 1;  // 3 bit length + 3 bits of offset in opcode
    static final int COPY_2_BYTE_OFFSET = 2;

    static final int MAX_SNAPPY_HASH_TABLE_BITS = 14;
    public static final int MAX_SNAPPY_HASH_TABLE_SIZE = 1 << MAX_SNAPPY_HASH_TABLE_BITS;

    private UnsafeSnappyConstants() {}
}
