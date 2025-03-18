package org.apache.lucene.luke.app.desktop.exjobb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TextFileBenchmarkDataOutput implements BenchmarkDataOutput {
    final static String dataFolderPath = "../BenchmarkData/";

    @Override
    public void printStatus() {
        System.out.println("Storing benchmark data in text file ");
    }

    @Override
    public void write(List<BenchmarkPerformanceData> benchmarkPerformanceData, Dataset dataset) throws IOException {
        // Specify the folder path
        File folder = new File(dataFolderPath);

        // Create the folder
        boolean isCreated = folder.mkdir();

        try (FileWriter writer = new FileWriter(dataFolderPath + dataset.name() + ".csv")) {
            // Write the header
            writer.append("Compression Technique, Index Size(MB), Indexing Time(ms), Average Query Time(ns)\n");


            for (BenchmarkPerformanceData benchmarkData : benchmarkPerformanceData) {
                writer.append(benchmarkData.type.name() + "," + benchmarkData.indexingBenchmarkData.totalIndexSizeInMB + "," +
                        benchmarkData.indexingBenchmarkData.totalIndexingTimeInMS + "," + benchmarkData.searchBenchmarkData.averageQuerySearchTimeInMS + "\n");
            }

            System.out.println("CSV file created successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
