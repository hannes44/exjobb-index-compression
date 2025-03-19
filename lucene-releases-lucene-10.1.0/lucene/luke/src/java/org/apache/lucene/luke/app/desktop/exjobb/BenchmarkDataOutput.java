package org.apache.lucene.luke.app.desktop.exjobb;

import java.io.IOException;
import java.util.List;

public interface BenchmarkDataOutput {
    // Prints where the output is printed, saved
    public void printStatus();

    public void write(IndexingBenchmarkData data, Dataset dataset) throws IOException;

    public void write(SearchBenchmarkData data, Dataset dataset) throws IOException;
}
