package org.apache.lucene.luke.app.desktop.exjobb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BenchmarkUtils {

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

    public static long getIndexSizeInMB()
    {
        return 1;
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
}
