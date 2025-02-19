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
            deleteExistingIndex(indexPath);

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

            DatasetCompressionBenchmarker benchmarker = new RandomWordsBenchmarker();

            benchmarker.BenchmarkIndexing(writer);

            benchmarker.BenchmarkSearching("index");
            
            if (true)
                return;


        } catch (IOException e) {
            e.printStackTrace();
        }
        Path indexPathPath = Paths.get(indexPath);
        // Calculate and print the size of the index
        long indexSize = getIndexSize(indexPathPath);
        System.out.println("Total size of the index: " + formatSize(indexSize));

        SearchTest(indexPath);
    }

    public static void deleteExistingIndex(String indexPath)
    {
        // Path to the index directory
        Path indexDirectoryPath = Paths.get(indexPath);
        try {
            // Delete the entire directory and its contents
            if (Files.exists(indexDirectoryPath)) {
                Files.walk(indexDirectoryPath)
                        .sorted((path1, path2) -> path2.compareTo(path1))  // Delete files first, then the directory
                        .forEach(path -> {
                            try {
                                Files.delete(path); // Delete each file and subdirectory
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                System.out.println("Index directory and all files deleted.");
            } else {
                System.out.println("Index directory does not exist.");
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Calculates the total size of the index directory by summing the size of all files.
     * @param indexPath Path to the index directory
     * @return The total size in bytes
     */
    public static long getIndexSize(Path indexPath) {
        File indexDir = indexPath.toFile();
        long totalSize = 0;

        // Traverse the index directory and sum the sizes of all files
        if (indexDir.exists() && indexDir.isDirectory()) {
            File[] files = indexDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    totalSize += file.length();
                }
            }
        }
        return totalSize;
    }

    /**
     * Formats the size in a human-readable format.
     * @param size The size in bytes
     * @return A formatted size string (e.g., "2.5 MB")
     */
    public static String formatSize(long size) {
        if (size <= 0) return "0 Bytes";

        String[] units = {"Bytes", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;

        double sizeInUnit = size;
        while (sizeInUnit >= 1024 && unitIndex < units.length - 1) {
            sizeInUnit /= 1024;
            unitIndex++;
        }

        return String.format("%.2f %s", sizeInUnit, units[unitIndex]);
    }

    public static void SearchTest(String indexPath)
    {

    }

}
