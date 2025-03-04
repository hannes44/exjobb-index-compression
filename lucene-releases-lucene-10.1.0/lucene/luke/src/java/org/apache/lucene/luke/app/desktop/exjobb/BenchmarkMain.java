package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
//import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter.TermCompressionMode;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class BenchmarkMain {
    public static void entryPoint()
    {
        luceneCompressionTesting();
    }

    public static void luceneCompressionTesting()
    {
        String indexPath = "index";
        try {
            BenchmarkUtils.deleteExistingIndex(indexPath);

            // Directory where the index will be stored
            Directory directory = FSDirectory.open(Paths.get(indexPath));

            // Standard analyzer for text processing
            StandardAnalyzer analyzer = new StandardAnalyzer();

            // Index writer configuration
            IndexWriterConfig config = new IndexWriterConfig(analyzer);


            boolean benchmarkSearch = false;
            boolean useDefaultLuceneCompression = false;
            if (useDefaultLuceneCompression)
                config.setCodec(new Lucene101Codec());
            else
                config.setCodec(new Lucene101Codec(Lucene101Codec.Mode.BEST_SPEED, IntegerCompressionType.DEFAULT, TermCompressionMode.LZ4));


            IndexWriter writer = new IndexWriter(directory, config);

            DatasetCompressionBenchmarker benchmarker = new CommonCrawlBenchmarker();

            IndexingBenchmarkData indexingData = benchmarker.BenchmarkIndexing(writer);

            SearchBenchmarkData searchData = null;

            if (benchmarkSearch) {
                searchData = benchmarker.BenchmarkSearching("index");
            }

            System.out.println("Benchmark for dataset: " + benchmarker.GetDatasetName());
            System.out.println("Indexing Time In MS: " + indexingData.totalIndexingTimeInMS);
            System.out.println("Index Size In MB: " + indexingData.totalIndexSizeInMB);
            if (benchmarkSearch) {
                System.out.println("Average Search query speed in MS: " + searchData.averageQuerySearchTimeInMS);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
