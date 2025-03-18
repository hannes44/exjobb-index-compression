/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.snappy;

import java.io.IOException;
import java.io.InputStream;

final class SnappyInternalUtils
{
    private SnappyInternalUtils()
    {
    }

    //
    // Copied from Guava Preconditions
    static <T> T checkNotNull(T reference, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (reference == null) {
            // If either of these parameters is null, the right thing happens anyway
            throw new NullPointerException(String.format(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }

    static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    static void checkPositionIndexes(int start, int end, int size)
    {
        // Carefully optimized for execution by hotspot (explanatory comment above)
        if (start < 0 || end < start || end > size) {
            throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
        }
    }

    static String badPositionIndexes(int start, int end, int size)
    {
        if (start < 0 || start > size) {
            return badPositionIndex(start, size, "start index");
        }
        if (end < 0 || end > size) {
            return badPositionIndex(end, size, "end index");
        }
        // end < start
        return String.format("end index (%s) must not be less than start index (%s)", end, start);
    }

    static String badPositionIndex(int index, int size, String desc)
    {
        if (index < 0) {
            return String.format("%s (%s) must not be negative", desc, index);
        }
        else if (size < 0) {
            throw new IllegalArgumentException("negative size: " + size);
        }
        else { // index > size
            return String.format("%s (%s) must not be greater than size (%s)", desc, index, size);
        }
    }
}
