package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
//import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.codecs.exjobb.integercompression.LimitTestCompressor;
import org.apache.lucene.util.compress.TermCompressionMode;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

import java.util.ArrayList;

public class BenchmarkMain {
    private static final Dataset defaultDataset = Dataset.COMMONCRAWL_2025;
    private static final Queryset defaultQueryset = Queryset.RANDOMWORDS;
    private static final IntegerCompressionType defaultIntegerCompressionType = IntegerCompressionType.DEFAULT;
    private static final TermCompressionMode defaultTermCompressionMode = TermCompressionMode.SNAPPY;
    private static final BenchmarkingType defaultBenchmarkingType = BenchmarkingType.INDEXING;

    // Can only benchmark either searching or indexing during a run since we don't
    // want caching to interfere
    public enum BenchmarkingType {
        SEARCH,
        INDEXING
    }

    // args[0]: BenchmarkingType, args[1]: Dataset: args[2]: IntegerCompressionType,
    // args[3] TermCompressionMode
    public static void entryPoint(String[] args) {
        BenchmarkingType benchmarkingType = defaultBenchmarkingType;
        Dataset dataset = defaultDataset;
        Queryset queryset = defaultQueryset;
        IntegerCompressionType integerCompressionType = defaultIntegerCompressionType;
        TermCompressionMode termCompressionMode = defaultTermCompressionMode;

        for (int i = 0; i < args.length; i++) {
            if (i == 0)
                benchmarkingType = BenchmarkingType.valueOf(args[i]);
            else if (i == 1)
                dataset = Dataset.valueOf(args[i]);
            else if (i == 2)
                queryset = Queryset.valueOf(args[i]);
            else if (i == 3)
                integerCompressionType = IntegerCompressionType.valueOf(args[i]);
            else if (i == 4)
                termCompressionMode = TermCompressionMode.valueOf(args[i]);
        }
        try {
            luceneCompressionTesting(dataset, queryset, integerCompressionType, termCompressionMode, benchmarkingType);
        } catch (IOException e) {
            System.out.println(e);
        }

        System.out.println(LimitTestCompressor.averageShortTime + " SHORT");
        System.out.println(LimitTestCompressor.averageIntTime + " INT");
    }

    public static void luceneCompressionTesting(Dataset dataset, Queryset queryset, IntegerCompressionType integerCompressionType,
            TermCompressionMode termMode, BenchmarkingType benchmarkingType) throws IOException {
        // For testing a specific setup, set to false. For complete testing of all
        // compression types on all datasets, set to true
        BenchmarkDataOutput output = new TextFileBenchmarkDataOutput();
        ArrayList<BenchmarkPerformanceData> benchmarkPerformanceDatas = new ArrayList<>();

        if (dataset == null)
            dataset = defaultDataset;
        if (queryset == null)
            queryset = defaultQueryset;
        if (integerCompressionType == null)
            integerCompressionType = defaultIntegerCompressionType;
        if (termMode == null)
            termMode = defaultTermCompressionMode;

        System.out.println("Benchmarking dataset: " + dataset.name() + ". Current queryset: " + queryset.name() + ". Using integer compression algorithm: "
                + integerCompressionType.name() + " And using term compression algorithm: " + termMode.name());

        if (benchmarkingType == BenchmarkingType.INDEXING) {
            IndexingBenchmarkData indexData = benchmarkIndexing(integerCompressionType, dataset, queryset, termMode);
            output.write(indexData, dataset);
        } else if (benchmarkingType == BenchmarkingType.SEARCH) {
            SearchBenchmarkData searchData = benchmarkSearching(integerCompressionType, dataset, queryset, termMode);
            output.write(searchData, dataset);
        }

    }

    public static IndexingBenchmarkData benchmarkIndexing(IntegerCompressionType type, Dataset dataset, Queryset queryset,
            TermCompressionMode termCompressionMode) throws IOException {
        String indexPath = "index/" + dataset.name() + "_" + type.name() + "_" + termCompressionMode.name();

        BenchmarkUtils.deleteExistingIndex(indexPath);

        // Directory where the index will be stored
        Directory directory = FSDirectory.open(Paths.get(indexPath));

        // Standard analyzer for text processing
        StandardAnalyzer analyzer = new StandardAnalyzer();

        // Index writer configuration
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        boolean useDefaultLuceneCompression = false;
        if (useDefaultLuceneCompression)
            config.setCodec(new Lucene101Codec());
        else
            config.setCodec(new Lucene101Codec(Lucene101Codec.Mode.BEST_SPEED, type, termCompressionMode));

        IndexWriter writer = new IndexWriter(directory, config);

        DatasetCompressionBenchmarker benchmarker = getBenchmarker(dataset, queryset);

        IndexingBenchmarkData indexingData = benchmarker.BenchmarkIndexing(writer, indexPath);
        indexingData.integerCompressionType = type;
        indexingData.termCompressionMode = termCompressionMode;

        System.out.println("Benchmark for dataset: " + benchmarker.GetDatasetName());
        System.out.println("Indexing Time In MS: " + indexingData.totalIndexingTimeInMS);
        System.out.println("Index Size In MB: " + indexingData.totalIndexSizeInMB);

        return indexingData;
    }

    public static SearchBenchmarkData benchmarkSearching(IntegerCompressionType type, Dataset dataset, Queryset queryset,
            TermCompressionMode termCompressionMode) throws IOException {
        String indexPath = "index/" + dataset.name() + "_" + type.name() + "_" + termCompressionMode.name();
        DatasetCompressionBenchmarker benchmarker = getBenchmarker(dataset, queryset);

        Lucene101Codec.integerCompressionType = type;

        Lucene101Codec.termCompressionMode = termCompressionMode;

        SearchBenchmarkData searchBenchmarkData = benchmarker.BenchmarkSearching(indexPath);

        searchBenchmarkData.integerCompressionType = type;
        searchBenchmarkData.termCompressionMode = termCompressionMode;

        return searchBenchmarkData;
    }

    public static DatasetCompressionBenchmarker getBenchmarker(Dataset dataset, Queryset queryset) {
        switch (dataset) {
            case COMMONCRAWL_2025:
                return switch (queryset) {
                    case RANDOMWORDS ->
                            new CommonCrawlBenchmarker("../Datasets/CommonCrawl-2025-05", "../Datasets/Queries/10000Words.txt");
                    case TRECMILLION ->
                            new CommonCrawlBenchmarker("../Datasets/CommonCrawl-2025-05", "../Datasets/Queries/09.mq.topics.20001-60000.txt");
                };
            case COMMONCRAWL_2014:
                return switch (queryset) {
                    case RANDOMWORDS ->
                            new CommonCrawlBenchmarker("../Datasets/CommonCrawl-2014-10", "../Datasets/Queries/10000Words.txt");
                    case TRECMILLION ->
                            new CommonCrawlBenchmarker("../Datasets/CommonCrawl-2014-10", "../Datasets/Queries/09.mq.topics.20001-60000.txt");
                };
            default:
                System.out.println("ERROR, The given dataset is not supported for benchmarking");
                return null;
        }

    }

}
