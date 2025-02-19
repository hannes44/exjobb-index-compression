package org.apache.lucene.luke.app.desktop.benchmark.commoncrawl;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.surround.parser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;


public class CommonCrawlBenchmarker {

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

    public interface WETHandler {
        void handleRecord(String url, String content);
    }

    public static void BenchmarkIndexingCommonCrawl(IndexWriter indexWriter) {
        long startTime = System.currentTimeMillis();

        File folder = new File(folderPath);
        File[] wetFiles = folder.listFiles((dir, name) -> name.endsWith(".wet")); // Filter for .wet files
        int fileCount = wetFiles.length;

        if (wetFiles == null || wetFiles.length == 0) {
            System.out.println("No WET files found in the folder: " + folderPath);
            return;
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
            try {
                System.out.println("Processing file: " + wetFile.getName());
                System.out.print("File: " + (totalFilesProcessed + 1) + " Out of: " + fileCount);
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
    }

    private static final String INDEX_PATH = "index"; // Update this
    private static final int WARMUP_QUERIES = 5;
    private static final int MEASURED_QUERIES = 50;

    public static void BenchmarkSearchingCommonCrawl() throws IOException, ParseException {



        try (FSDirectory directory = FSDirectory.open(Paths.get(INDEX_PATH));

             DirectoryReader reader = DirectoryReader.open(directory)) {

            IndexSearcher searcher = new IndexSearcher(reader);
            // searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

            org.apache.lucene.queryparser.classic.QueryParser parser = new QueryParser("content", new StandardAnalyzer());

            // Sample queries (modify as needed)
            List<String> queries = List.of(
                    "climate change",
                    "machine learning",
                    "blockchain technology",
                    "global economy",
                    "open source software"
            );

            System.out.println("Warming up...");
            for (int i = 0; i < WARMUP_QUERIES; i++) {
                runQuery(searcher, parser.parse(queries.get(i % queries.size())), false);
            }

            System.out.println("Benchmarking...");
            long totalTime = 0;
            for (int i = 0; i < MEASURED_QUERIES; i++) {
                long startTime = System.nanoTime();
                runQuery(searcher, parser.parse(queries.get(i % queries.size())), true);
                long elapsedTime = System.nanoTime() - startTime;
                totalTime += elapsedTime;
            }

            double avgTimeMs = (totalTime / 1_000_000.0) / MEASURED_QUERIES;
            System.out.println("Average Query Time: " + avgTimeMs + " ms");

        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            e.printStackTrace();
        }

    }

    private static void runQuery(IndexSearcher searcher, Query query, boolean measure) throws IOException {
        long start = System.nanoTime();
        TopDocs topDocs = searcher.search(query, 10);
        long end = System.nanoTime();

        if (measure) {
            System.out.println("Query: " + query.toString() + " | Time: " + (end - start) / 1_000_000.0 + " ms");
        }
    }

    }
