/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress.unsafeZstd;

class UnsafeHuffmanCompressionContext
{
    private final UnsafeHuffmanTableWriterWorkspace tableWriterWorkspace = new UnsafeHuffmanTableWriterWorkspace();
    private final UnsafeHuffmanCompressionTableWorkspace compressionTableWorkspace = new UnsafeHuffmanCompressionTableWorkspace();

    private UnsafeHuffmanCompressionTable previousTable = new UnsafeHuffmanCompressionTable(UnsafeHuffman.MAX_SYMBOL_COUNT);
    private UnsafeHuffmanCompressionTable temporaryTable = new UnsafeHuffmanCompressionTable(UnsafeHuffman.MAX_SYMBOL_COUNT);

    private UnsafeHuffmanCompressionTable previousCandidate = previousTable;
    private UnsafeHuffmanCompressionTable temporaryCandidate = temporaryTable;

    public UnsafeHuffmanCompressionTable getPreviousTable()
    {
        return previousTable;
    }

    public UnsafeHuffmanCompressionTable borrowTemporaryTable()
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

    public UnsafeHuffmanCompressionTableWorkspace getCompressionTableWorkspace()
    {
        return compressionTableWorkspace;
    }

    public UnsafeHuffmanTableWriterWorkspace getTableWriterWorkspace()
    {
        return tableWriterWorkspace;
    }
}