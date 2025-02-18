package org.apache.lucene.codecs.benchmarking;

import org.apache.lucene.codecs.lucene912.IntegerCompressor;

// Benchmaker for specific dataset
public interface DatasetCompressionBenchmarker {
    /**
     *
     */
    public String GetDatasetName();

    public int GetDatasetSizeInMB();

    public void BenchmarkIndexing();

    public void BenchmarkSearching();
}
