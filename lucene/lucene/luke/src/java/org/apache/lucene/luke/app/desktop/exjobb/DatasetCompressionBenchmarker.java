package org.apache.lucene.luke.app.desktop.exjobb;


import org.apache.lucene.index.IndexWriter;

import java.io.IOException;

// Benchmaker for specific dataset
public interface DatasetCompressionBenchmarker {
    /**
     *
     */
    public String GetDatasetName();

    public IndexingBenchmarkData BenchmarkIndexing(IndexWriter writer) throws IOException;

    public SearchBenchmarkData BenchmarkSearching(String indexPath);
}
