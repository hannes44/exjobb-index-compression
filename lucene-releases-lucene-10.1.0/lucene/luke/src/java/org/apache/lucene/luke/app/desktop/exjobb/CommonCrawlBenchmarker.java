package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.luke.app.desktop.exjobb.BenchmarkUtils;
import org.apache.lucene.luke.app.desktop.exjobb.DatasetCompressionBenchmarker;
import org.apache.lucene.luke.app.desktop.exjobb.IndexingBenchmarkData;
import org.apache.lucene.luke.app.desktop.exjobb.SearchBenchmarkData;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.surround.parser.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class CommonCrawlBenchmarker implements DatasetCompressionBenchmarker {

    final static String folderPath = "../Datasets/CommonCrawl-2025-05";
    public static void parseWETFile(String filePath, WETHandler handler) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            StringBuilder content = new StringBuilder();
            String url = null;
            boolean inContent = false;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("WARC-Target-URI:")) {
                    url = line.substring("WARC-Target-URI:".length()).trim();
                } else if (line.isEmpty()) {
                    inContent = true;
                } else if (inContent) {
                    content.append(line).append("\n");
                }

                // End of a record
                if (line.startsWith("WARC/1.0") && url != null && content.length() > 0) {
                    handler.handleRecord(url, content.toString());
                    url = null;
                    content.setLength(0);
                    inContent = false;
                }
            }
        }
    }

    @Override
    public String GetDatasetName() {
        return "CommonCrawl-2025-05";
    }

    @Override
    public IndexingBenchmarkData BenchmarkIndexing(IndexWriter indexWriter) {
        int maxFiles = 10;

        long startTime = System.currentTimeMillis();

        File folder = new File(folderPath);
        File[] wetFiles = folder.listFiles((dir, name) -> name.endsWith(".wet")); // Filter for .wet files
        int fileCount = wetFiles.length;

        int filesToIndex = maxFiles > fileCount ? fileCount : maxFiles;

        if (wetFiles == null || wetFiles.length == 0) {
            System.out.println("No WET files found in the folder: " + folderPath);
            return null;
        }

        // Implement the WETHandler to index the content
        WETHandler handler = new WETHandler() {
            @Override
            public void handleRecord(String url, String content) {
                try {
                    // Create a Lucene document
                    Document doc = new Document();
                    doc.add(new StringField("url", url, Field.Store.YES)); // Store the URL
                    doc.add(new TextField("content", content, Field.Store.NO)); // Store the content

                    // Add the document to the index
                    indexWriter.addDocument(doc);
                } catch (IOException e) {
                    System.err.println("Failed to index URL: " + url);
                    e.printStackTrace();
                }
            }
        };

        int totalFilesProcessed = 0;
        for (File wetFile : wetFiles) {
            if (filesToIndex <= totalFilesProcessed)
                break;

            try {
                System.out.println("Processing file: " + wetFile.getName());
                System.out.print("File: " + (totalFilesProcessed + 1) + " Out of: " + filesToIndex);
                parseWETFile(wetFile.getAbsolutePath(), handler);
                totalFilesProcessed++;
            } catch (IOException e) {
                System.err.println("Failed to process file: " + wetFile.getName());
                e.printStackTrace();
            }
        }

        // Commit and close the index writer
        try {
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            System.err.println("Failed to commit or close the index writer.");
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Indexing completed for " + totalFilesProcessed + " files in " + duration + " milliseconds.");

        IndexingBenchmarkData result = new IndexingBenchmarkData();
        result.totalIndexingTimeInMS = duration;
        result.totalIndexSizeInMB = BenchmarkUtils.getIndexSizeInMB(INDEX_PATH);

        return result;
    }

    private static final String INDEX_PATH = "index";
    private static final int WARMUP_QUERIES = 5;
    private static final int MEASURED_QUERIES = 50;

    @Override
    public SearchBenchmarkData BenchmarkSearching(String indexPath) {
        SearchBenchmarkData benchmarkData = new SearchBenchmarkData();
        List<Long> queryTimes = new ArrayList<>();

        try {
            // Open the index
            Directory directory = FSDirectory.open(Paths.get(indexPath));
            DirectoryReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // Define your 8-layer deep Boolean query
            BooleanQuery booleanQuery = createDeepBooleanQuery(8);

            // Warm-up: Run the query once to warm up the JVM
            searcher.search(booleanQuery, 10);

            // Benchmark: Run the query multiple times and record the time taken
            int iterations = 10; // Number of times to run the query
            long totalTime = 0;
            for (int i = 0; i < iterations; i++) {
                long startTime = System.nanoTime();
                TopDocs results = searcher.search(booleanQuery, 10);
                long endTime = System.nanoTime();
                totalTime += (endTime - startTime);
            }
            long averageTime = totalTime / iterations;
            queryTimes.add(averageTime);

            // Close the reader
            reader.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

        long averageTime = 0;
        for (Long time : queryTimes)
        {
            averageTime += time;
        }
        averageTime /= queryTimes.size();
        benchmarkData.averageQuerySearchTimeInMS = averageTime;
        return benchmarkData;
    }

    private BooleanQuery createDeepBooleanQuery(int depth) {
        if (depth <= 0) {
            throw new IllegalArgumentException("Depth must be greater than 0");
        }

        // Base case: Create a simple TermQuery at the deepest level
        if (depth == 1) {
            return new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("content", "term" + depth)), BooleanClause.Occur.MUST)
                    .build();
        }

        // Recursive case: Nest another BooleanQuery inside
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(createDeepBooleanQuery(depth - 1), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("content", "term" + depth)), BooleanClause.Occur.MUST);
        return builder.build();
    }

    public interface WETHandler {
        void handleRecord(String url, String content);
    }

    private static void runQuery(IndexSearcher searcher, Query query, boolean measure) throws IOException {
        long start = System.nanoTime();
        TopDocs topDocs = searcher.search(query, 10);

        long end = System.nanoTime();

        if (measure) {
            System.out.println("Query: " + query.toString() + " | Time: " + (end - start) / 1_000_000.0 + " ms" + "Total Hits: " + topDocs.totalHits);
        }
    }

}
