/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

class RepeatedOffsets
{
    private int offset0 = 1;
    private int offset1 = 4;

    private int tempOffset0;
    private int tempOffset1;

    public int getOffset0()
    {
        return offset0;
    }

    public int getOffset1()
    {
        return offset1;
    }

    public void saveOffset0(int offset)
    {
        tempOffset0 = offset;
    }

    public void saveOffset1(int offset)
    {
        tempOffset1 = offset;
    }

    public void commit()
    {
        offset0 = tempOffset0;
        offset1 = tempOffset1;
    }
}