package org.apache.lucene.luke.app.desktop.exjobb;

import java.io.IOException;
import java.util.List;

public interface BenchmarkDataOutput {
    // Prints where the output is printed, saved
    public void printStatus();

    public void write(List<BenchmarkPerformanceData> benchmarkPerformanceData, Dataset dataset) throws IOException;
}
