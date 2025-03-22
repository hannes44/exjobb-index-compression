package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TextFileBenchmarkDataOutput implements BenchmarkDataOutput {
    final static String dataFolderPath = "../BenchmarkData/";

    @Override
    public void printStatus() {
        System.out.println("Storing benchmark data in text file ");
    }

    @Override
    public void write(IndexingBenchmarkData data, Dataset dataset) throws IOException {
        // Specify the folder path
        File folder = new File(dataFolderPath);
        folder.mkdir();
        String indexDataFolderPath = dataFolderPath + "IndexingData/";
        File indexingDataFolder = new File(indexDataFolderPath);
        indexingDataFolder.mkdir();

        try (FileWriter writer = new FileWriter(indexDataFolderPath + dataset.name() + ".csv", true)) {

            writer.append(data.integerCompressionType.name() + "," + data.totalIndexSizeInMB + "," + data.totalIndexingTimeInMS +  "\n");
        }
    }

    @Override
    public void write(SearchBenchmarkData data, Dataset dataset) throws IOException {
        String searchDataFolderPath = dataFolderPath + "SearchData/";
        File indexingDataFolder = new File(searchDataFolderPath);
        indexingDataFolder.mkdir();

        try (FileWriter writer = new FileWriter(searchDataFolderPath + dataset.name() + ".csv", true)) {

            writer.append(data.integerCompressionType.name() + "," + data.averageQuerySearchTimeInNS +  "\n");
        }
    }

    public void writeSearchData(SearchBenchmarkData data, Dataset dataset, IntegerCompressionType integerCompressionType) throws IOException {

    }
}
