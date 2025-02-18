package org.apache.lucene.codecs.benchmarking.commoncrawl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CommonCrawlBenchmarker {
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
        String folderPath = "../Datasets/CommonCrawl-2025-05";

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
}
