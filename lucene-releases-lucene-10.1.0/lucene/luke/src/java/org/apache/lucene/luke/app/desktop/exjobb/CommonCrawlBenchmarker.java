package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


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
    public IndexingBenchmarkData BenchmarkIndexing(IndexWriter indexWriter, String indexPath) {
        double maxSizeGB = 100; // Default to 1GB if not specified
        long maxSizeBytes = (long) (maxSizeGB * 1024 * 1024 * 1024);

        long startTime = System.currentTimeMillis();
        File folder = new File(folderPath);
        File[] wetFiles = folder.listFiles((dir, name) -> name.endsWith(".wet"));

        if (wetFiles == null || wetFiles.length == 0) {
            System.out.println("No WET files found in the folder: " + folderPath);
            return null;
        }

        // Implement the WETHandler
        WETHandler handler = new WETHandler() {
            @Override
            public void handleRecord(String url, String content) {
                try {
                    Document doc = new Document();
                    doc.add(new StringField("url", url, Field.Store.YES));
                    doc.add(new TextField("content", content, Field.Store.NO));
                    indexWriter.addDocument(doc);
                } catch (IOException e) {
                    System.err.println("Failed to index URL: " + url);
                    e.printStackTrace();
                }
            }
        };

        int totalFilesProcessed = 0;
        long totalProcessedSize = 0;
        boolean shouldStop = false;

        for (File wetFile : wetFiles) {
            if (shouldStop) {
                break;
            }

            long fileSize = wetFile.length();

            // Check if processing this file would exceed the limit (but process it anyway)
            if (totalProcessedSize + fileSize > maxSizeBytes) {
                shouldStop = true;
                System.out.println("Next file would exceed limit, but processing it anyway...");
            }

            try {
                System.out.printf("Processing: %s (%.2f MB)%n",
                        wetFile.getName(), fileSize / (1024.0 * 1024.0));

                parseWETFile(wetFile.getAbsolutePath(), handler);
                totalFilesProcessed++;
                totalProcessedSize += fileSize;

                System.out.printf("Cumulative: %.2f/%.2f GB%n",
                        totalProcessedSize / (1024.0 * 1024.0 * 1024.0),
                        maxSizeGB);

            } catch (IOException e) {
                System.err.println("Failed to process file: " + wetFile.getName());
                e.printStackTrace();
            }
        }

        try {
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            System.err.println("Failed to commit or close the index writer.");
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.printf(
                "Indexing completed for %d files (%.2f GB input) in %d ms.%n" +
                        "Final size: %.2f GB (%.2f GB over limit)%n",
                totalFilesProcessed,
                totalProcessedSize / (1024.0 * 1024.0 * 1024.0),
                duration,
                totalProcessedSize / (1024.0 * 1024.0 * 1024.0),
                Math.max(0, (totalProcessedSize - maxSizeBytes) / (1024.0 * 1024.0 * 1024.0))
        );

        IndexingBenchmarkData result = new IndexingBenchmarkData();
        result.totalIndexingTimeInMS = duration;
        result.totalIndexSizeInMB = BenchmarkUtils.getIndexSizeInMB(indexPath);

        return result;
    }

    private static final String INDEX_PATH = "index";
    private static final int WARMUP_QUERIES = 5;
    private static final int MEASURED_QUERIES = 50;

    @Override
    public SearchBenchmarkData BenchmarkSearching(String indexPath) {
        SearchBenchmarkData benchmarkData = new SearchBenchmarkData();
        try {
            // Open the index
            Directory directory = FSDirectory.open(Paths.get(indexPath));
            DirectoryReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // Define a list of queries to benchmark
            List<Query> queries = createCommonCrawlQueries();

            // Warm-up: Run each query once to warm up the JVM
            for (Query query : queries) {
                searcher.search(query, 10);
            }

            // Benchmark: Run each query multiple times and record the time taken
            int iterations = 1; // Number of times to run each query
            List<Long> queryTimes = new ArrayList<>();
            List<Long> queryHits = new ArrayList<>();

            for (Query query : queries) {
                long totalTime = 0;
                long hits = 0;
                for (int i = 0; i < iterations; i++) {
                    long startTime = System.nanoTime();
                    TopDocs results = searcher.search(query, 10);
                    long endTime = System.nanoTime();
                    totalTime += (endTime - startTime);
                    hits += results.totalHits.value();
                }
                long averageTime = totalTime / iterations;
                queryTimes.add(averageTime);
                long averageHits = hits / iterations;
                queryHits.add(averageHits);
            }

            // Print results
            System.out.println("Benchmark Results:");
            for (int i = 0; i < queries.size(); i++) {
                System.out.println("Query " + (i + 1) + ": " + queries.get(i) + " | Average Hits: " + queryHits.get(i) + " | Average Time: " + queryTimes.get(i) + " ns");
            }

            // Close the reader
            reader.close();
            long totalTime = 0;
            for (Long time : queryTimes) {
                totalTime += time;
            }
            long averageTime = totalTime / queryTimes.size();
            benchmarkData.averageQuerySearchTimeInNS = averageTime;
            System.out.println("Average query time in ns:" + averageTime);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return benchmarkData;
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

    private List<Query> createCommonCrawlQueries() throws Exception {
        List<Query> queries = new ArrayList<>();
        StandardAnalyzer analyzer = new StandardAnalyzer();

        String filePath = "../Datasets/10000Words.txt";

        // List to store words temporarily
        List<String> wordList = new ArrayList<>();

        long seed = 12345L; // Fixed seed value
        Random random = new Random(seed);



        // Read the file
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                wordList.add(line); // Add each word to the list
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Get a random index
        int randomIndex = random.nextInt(wordList.size());

        for (String word : wordList) {
            // 1. Simple Term Query (Search for a specific word in the content)
          //  queries.add(new TermQuery(new Term("content", word)));

            // Create a SpanNearQuery for proximity search
            SpanTermQuery term1 = new SpanTermQuery(new Term("content", word));
            SpanTermQuery term2 = new SpanTermQuery(new Term("content", wordList.get(randomIndex)));
            int slop = 3; // Maximum allowed distance between terms
            boolean inOrder = true; // Terms must appear in the specified order
            SpanNearQuery proximityQuery = new SpanNearQuery(new SpanQuery[]{term1, term2}, slop, inOrder);
            queries.add(proximityQuery);
        }



        /*


        // 2. Boolean Query (AND) (Search for pages containing both "artificial" and "intelligence")
        queries.add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("content", "artificial")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("content", "intelligence")), BooleanClause.Occur.MUST)
                .build());

        // 3. Boolean Query (OR) (Search for pages containing either "covid" or "pandemic")
        queries.add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("content", "covid")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("content", "pandemic")), BooleanClause.Occur.SHOULD)
                .build());

        // 4. Phrase Query (Search for the exact phrase "climate change")
        queries.add(new PhraseQuery.Builder()
                .add(new Term("content", "climate"))
                .add(new Term("content", "change"))
                .build());

        // 5. Wildcard Query (Search for terms starting with "tech")
        queries.add(new WildcardQuery(new Term("content", "tech*")));

        // 8. Prefix Query (Search for domains starting with "news")
        queries.add(new PrefixQuery(new Term("domain", "news")));
         */

        return queries;
    }

}
