/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util;

public class MalformedInputException
        extends RuntimeException
{
    private final long offset;

    public MalformedInputException(long offset)
    {
        this(offset, "Malformed input");
    }

    public MalformedInputException(long offset, String reason)
    {
        super(reason + ": offset=" + offset);
        this.offset = offset;
    }

    public long getOffset()
    {
        return offset;
    }
}