package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.luke.app.desktop.exjobb.commoncrawl.CommonCrawlBenchmarker;
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
import java.util.List;
import java.util.Random;

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


            boolean useDefaultLuceneCompression = true;
            if (useDefaultLuceneCompression)
                config.setCodec(new Lucene912Codec());
            else
                config.setCodec(new Lucene912Codec(Lucene912Codec.Mode.BEST_SPEED, IntegerCompressionType.DELTA));

            IndexWriter writer = new IndexWriter(directory, config);

            DatasetCompressionBenchmarker benchmarker = new CommonCrawlBenchmarker();

            IndexingBenchmarkData indexingData = benchmarker.BenchmarkIndexing(writer);

            SearchBenchmarkData searchData = benchmarker.BenchmarkSearching("index");

            System.out.println("Benchmark for dataset: " + benchmarker.GetDatasetName());
            System.out.println("Indexing Time In MS: " + indexingData.totalIndexingTimeInMS);
            System.out.println("Index Size In MB: " + indexingData.totalIndexSizeInMB);
            System.out.println("Average Search query speed in MS: " + searchData.averageQuerySearchTimeInMS);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
