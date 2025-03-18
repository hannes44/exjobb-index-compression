package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.backward_codecs.lucene912.Lucene912Codec;
//import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
//import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.luke.app.desktop.exjobb.CommonCrawlBenchmarker;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BenchmarkMain {
    public static void entryPoint()
    {
        luceneCompressionTesting();
    }

    public static void luceneCompressionTesting()
    {
        // For testing a specific setup, set to false. For complete testing of all compression types on all datasets, set to true
        boolean doCompleteBenchmark = false;

        BenchmarkDataOutput output = new TextFileBenchmarkDataOutput();

        if (doCompleteBenchmark)
            completeBenchmark(output);
        else {
            ArrayList<BenchmarkPerformanceData> benchmarkPerformanceDatas = new ArrayList<>();
            benchmarkPerformanceDatas.add(benchmarkCompressionType(IntegerCompressionType.DELTA, Dataset.CommonCrawl, output));
            benchmarkPerformanceDatas.add(benchmarkCompressionType(IntegerCompressionType.DEFAULT, Dataset.CommonCrawl, output));
            try {
                output.write(benchmarkPerformanceDatas, Dataset.CommonCrawl);
            }
            catch (IOException e) {

            }
        }
    }

    public static BenchmarkPerformanceData benchmarkCompressionType(IntegerCompressionType type, Dataset dataset, BenchmarkDataOutput output) {

        ArrayList<BenchmarkPerformanceData> benchmarkPerformanceDatas = new ArrayList<>();

        BenchmarkPerformanceData benchmarkPerformanceData = new BenchmarkPerformanceData();
        String indexPath = "index/" + dataset.name()  + "_" + type.name();
        try {
            BenchmarkUtils.deleteExistingIndex(indexPath);

            // Directory where the index will be stored
            Directory directory = FSDirectory.open(Paths.get(indexPath));

            // Standard analyzer for text processing
            StandardAnalyzer analyzer = new StandardAnalyzer();

            // Index writer configuration
            IndexWriterConfig config = new IndexWriterConfig(analyzer);

            config.setCodec(new Lucene101Codec(Lucene101Codec.Mode.BEST_SPEED, type));

            IndexWriter writer = new IndexWriter(directory, config);

            DatasetCompressionBenchmarker benchmarker = getBenchmarker(dataset);

            IndexingBenchmarkData indexingData = benchmarker.BenchmarkIndexing(writer, indexPath);

            SearchBenchmarkData searchData = benchmarker.BenchmarkSearching(indexPath);

            benchmarkPerformanceData.type = type;
            benchmarkPerformanceData.indexingBenchmarkData = indexingData;
            benchmarkPerformanceData.searchBenchmarkData = searchData;

            System.out.println("Benchmark for dataset: " + benchmarker.GetDatasetName());
            System.out.println("Indexing Time In MS: " + indexingData.totalIndexingTimeInMS);
            System.out.println("Index Size In MB: " + indexingData.totalIndexSizeInMB);
    //        System.out.println("Average Search query speed in MS: " + searchData.averageQuerySearchTimeInMS);

            benchmarkPerformanceDatas.add(benchmarkPerformanceData);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return benchmarkPerformanceData;
    }

    public static DatasetCompressionBenchmarker getBenchmarker(Dataset dataset) {
        switch (dataset) {
            case CommonCrawl:
                return new CommonCrawlBenchmarker();
            default:
                System.out.println("ERROR, The given dataset is not supported for benchmarking");
                return null;
        }


    }

    /**
     * Complete benchmarking testing all different algorithms and saves the result to a text file.
     */
    public static void completeBenchmark(BenchmarkDataOutput output) {
        // We start by calculating the baseline with the lucene default compression


        for (Dataset dataset : Dataset.values()) {
            ArrayList<BenchmarkPerformanceData> benchmarkPerformanceDatas = new ArrayList<>();
            for (IntegerCompressionType type : IntegerCompressionType.values()) {
                if (type == IntegerCompressionType.LIMITTEST2)
                    continue;
                System.out.println("Benchmarking " + dataset.name() + " using compreesion algorithm:" + type.name());
                BenchmarkPerformanceData data = benchmarkCompressionType(type, dataset, output);
                benchmarkPerformanceDatas.add(data);
            }

            try {
                output.write(benchmarkPerformanceDatas, dataset);
            } catch (IOException error)
            {

            }
        }
    }
}
