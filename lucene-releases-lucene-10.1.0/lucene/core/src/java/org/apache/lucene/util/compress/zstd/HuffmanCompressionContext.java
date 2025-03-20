/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.zstd;

import static org.apache.lucene.util.compress.zstd.ZSTDConstants.MAX_SYMBOL_COUNT;

class HuffmanCompressionContext
{
    private final HuffmanTableWriterWorkspace tableWriterWorkspace = new HuffmanTableWriterWorkspace();
    private final HuffmanCompressionTableWorkspace compressionTableWorkspace = new HuffmanCompressionTableWorkspace();

    private HuffmanCompressionTable previousTable = new HuffmanCompressionTable(MAX_SYMBOL_COUNT);
    private HuffmanCompressionTable temporaryTable = new HuffmanCompressionTable(MAX_SYMBOL_COUNT);

    private HuffmanCompressionTable previousCandidate = previousTable;
    private HuffmanCompressionTable temporaryCandidate = temporaryTable;

    public HuffmanCompressionTable getPreviousTable()
    {
        return previousTable;
    }

    public HuffmanCompressionTable borrowTemporaryTable()
    {
        previousCandidate = temporaryTable;
        temporaryCandidate = previousTable;

        return temporaryTable;
    }

    public void discardTemporaryTable()
    {
        previousCandidate = previousTable;
        temporaryCandidate = temporaryTable;
    }

    public void saveChanges()
    {
        temporaryTable = temporaryCandidate;
        previousTable = previousCandidate;
    }

    public HuffmanCompressionTableWorkspace getCompressionTableWorkspace()
    {
        return compressionTableWorkspace;
    }

    public HuffmanTableWriterWorkspace getTableWriterWorkspace()
    {
        return tableWriterWorkspace;
    }
}