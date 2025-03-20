package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;

// Benchmarking data for a specific dataset
public class BenchmarkPerformanceData {
    public IntegerCompressionType type;
    public IndexingBenchmarkData indexingBenchmarkData;
    public SearchBenchmarkData searchBenchmarkData;
}
