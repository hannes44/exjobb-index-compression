package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.luke.app.desktop.exjobb.BenchmarkUtils;
import org.apache.lucene.luke.app.desktop.exjobb.DatasetCompressionBenchmarker;
import org.apache.lucene.luke.app.desktop.exjobb.IndexingBenchmarkData;
import org.apache.lucene.luke.app.desktop.exjobb.SearchBenchmarkData;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
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
import java.time.Instant;
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
    public IndexingBenchmarkData BenchmarkIndexing(IndexWriter indexWriter, String indexPath) {
        int maxFiles = 1;

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
            int iterations = 10; // Number of times to run each query
            List<Long> queryTimes = new ArrayList<>();

            for (Query query : queries) {
                long totalTime = 0;
                for (int i = 0; i < iterations; i++) {
                    long startTime = System.nanoTime();
                    TopDocs results = searcher.search(query, 10);
                    long endTime = System.nanoTime();
                    totalTime += (endTime - startTime);
                }
                long averageTime = totalTime / iterations;
                queryTimes.add(averageTime);
            }

            // Print results
            System.out.println("Benchmark Results:");
            for (int i = 0; i < queries.size(); i++) {
                System.out.println("Query " + (i + 1) + ": " + queries.get(i) + " | Average Time: " + queryTimes.get(i) + " ns");
            }

            // Close the reader
            reader.close();
            long totalTime = 0;
            for (Long time : queryTimes) {
                totalTime += time;
            }
            long averageTime = totalTime / queryTimes.size();
            benchmarkData.averageQuerySearchTimeInMS = averageTime;

        } catch (Exception e) {
            e.printStackTrace();
        }

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

    private List<Query> createCommonCrawlQueries() throws Exception {
        List<Query> queries = new ArrayList<>();
        StandardAnalyzer analyzer = new StandardAnalyzer();

        // 1. Simple Term Query (Search for a specific word in the content)
        queries.add(new TermQuery(new Term("content", "artificial")));
        queries.add(new TermQuery(new Term("content", "intelligence")));


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

        // 6. Fuzzy Query (Search for terms similar to "machine" with a maximum edit distance of 2)
        queries.add(new FuzzyQuery(new Term("content", "machine"), 2));

        // 7. Range Query (Search for pages with a timestamp between 2020-01-01 and 2023-12-31)
        queries.add(LongPoint.newRangeQuery("timestamp",
                Instant.parse("2020-01-01T00:00:00Z").toEpochMilli(),
                Instant.parse("2023-12-31T23:59:59Z").toEpochMilli()));

        // 8. Prefix Query (Search for domains starting with "news")
        queries.add(new PrefixQuery(new Term("domain", "news")));

        // 9. Multi-Term Query (Search for "covid" in both the title and content fields)
        queries.add(new MultiFieldQueryParser(new String[]{"title", "content"}, analyzer).parse("covid"));

        // 10. Complex Query (Search for pages containing "covid" in the title, "vaccine" in the content, and published in 2021)
        queries.add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("title", "covid")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("content", "vaccine")), BooleanClause.Occur.MUST)
                .add(LongPoint.newRangeQuery("timestamp",
                        Instant.parse("2021-01-01T00:00:00Z").toEpochMilli(),
                        Instant.parse("2021-12-31T23:59:59Z").toEpochMilli()), BooleanClause.Occur.MUST)
                .build());
        */
        return queries;
    }

}
