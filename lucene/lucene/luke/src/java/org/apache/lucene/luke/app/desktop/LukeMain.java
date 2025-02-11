/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.luke.app.desktop;

import static org.apache.lucene.luke.app.desktop.util.ExceptionHandler.handle;

import java.awt.GraphicsEnvironment;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import javax.swing.UIManager;

import org.apache.lucene.codecs.customcodec.CustomCodec;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.luke.app.desktop.components.LukeWindowProvider;
import org.apache.lucene.luke.app.desktop.components.dialog.menubar.OpenIndexDialogFactory;
import org.apache.lucene.luke.app.desktop.util.DialogOpener;
import org.apache.lucene.luke.app.desktop.util.FontUtils;
import org.apache.lucene.luke.app.desktop.util.MessageUtils;
import org.apache.lucene.luke.util.LoggerFactory;
import javax.swing.JOptionPane;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.FileDescriptor;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import java.io.IOException;
import java.nio.file.*;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import java.nio.file.Paths;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;

/** Entry class for desktop Luke */
public class LukeMain {

  static {
    LoggerFactory.initGuiLogging();
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static JFrame frame;

  public static JFrame getOwnerFrame() {
    return frame;
  }

  /**
   * @return Returns {@code true} if GUI startup and initialization was successful.
   */
  private static boolean createGUI() {
    // uncaught error handler
    MessageBroker messageBroker = MessageBroker.getInstance();
    try {
      Thread.setDefaultUncaughtExceptionHandler((thread, cause) -> handle(cause, messageBroker));

      frame = new LukeWindowProvider().get();
      frame.setLocation(200, 100);
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);

      return true;
    } catch (Throwable e) {
      messageBroker.showUnknownErrorMessage();
      log.log(Level.SEVERE, "Cannot initialize components.", e);
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    luceneCompressionTesting();

    if (true)
      return;

    boolean sanityCheck = Arrays.asList(args).contains("--sanity-check");

    if (sanityCheck && GraphicsEnvironment.isHeadless()) {
      Logger.getGlobal().log(Level.SEVERE, "[Vader] Hello, Luke. Can't do much in headless mode.");
      Runtime.getRuntime().exit(0);
    }

    String lookAndFeelClassName = UIManager.getSystemLookAndFeelClassName();
    if (!lookAndFeelClassName.contains("AquaLookAndFeel")
        && !lookAndFeelClassName.contains("PlasticXPLookAndFeel")) {
      // may be running on linux platform
      lookAndFeelClassName = "javax.swing.plaf.metal.MetalLookAndFeel";
    }
    UIManager.setLookAndFeel(lookAndFeelClassName);

    GraphicsEnvironment genv = GraphicsEnvironment.getLocalGraphicsEnvironment();
    genv.registerFont(FontUtils.createElegantIconFont());


    var guiThreadResult = new SynchronousQueue<Boolean>();
    javax.swing.SwingUtilities.invokeLater(
        () -> {
          try {
            long _start = System.nanoTime();
            guiThreadResult.put(createGUI());

            // Show the initial dialog.
            OpenIndexDialogFactory openIndexDialogFactory = OpenIndexDialogFactory.getInstance();
            new DialogOpener<>(openIndexDialogFactory)
                .open(
                    MessageUtils.getLocalizedMessage("openindex.dialog.title"),
                    600,
                    420,
                    (factory) -> {});

            long _end = System.nanoTime() / 1_000_000;
            log.info(
                "Elapsed time for initializing GUI: "
                    + TimeUnit.NANOSECONDS.toMillis(_end - _start)
                    + " ms");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });



    if (Boolean.FALSE.equals(guiThreadResult.take())) {
      Logger.getGlobal().log(Level.SEVERE, "Luke could not start.");
      Runtime.getRuntime().exit(1);
    }

    if (sanityCheck) {
      // In sanity-check mode on non-headless displays, return success.
      Logger.getGlobal().log(Level.SEVERE, "[Vader] Hello, Luke. We seem to be fine.");
      Runtime.getRuntime().exit(0);
    }


  }

  public static void luceneCompressionTesting()
  {
    String indexPath = "index";
    try {
      deleteExistingIndex(indexPath);

      int numDocs = 3; // Number of documents to index

      // Directory where the index will be stored
      Directory directory = FSDirectory.open(Paths.get(indexPath));

      // Standard analyzer for text processing
      StandardAnalyzer analyzer = new StandardAnalyzer();

      // Index writer configuration
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setCodec(new CustomCodec());

      IndexWriter writer = new IndexWriter(directory, config);

      // Fixed seed for deterministic random content generation
      long seed = 12345L; // You can change this to any constant value
      Random random = new Random(seed);

      List<String> words = List.of(
              "abandon", "ability", "able", "absence", "absolute", "absorb", "abuse", "academy", "accept", "access",
              "accident", "account", "accuse", "achieve", "acquire", "address", "advance", "advice", "affect", "agency",
              "agenda", "airline", "alcohol", "allege", "alliance", "almost", "already", "amazing", "ancient", "animal",
              "announce", "anxiety", "appeal", "appreciate", "approve", "arrange", "arrival", "article", "assault", "assess",
              "assist", "assume", "athlete", "attract", "average", "balance", "banner", "barrier", "battery", "beautiful",
              "bicycle", "biology", "bother", "bottle", "bottom", "breathe", "brother", "budget", "bungee", "cabinet",
              "camera", "capture", "cattle", "center", "change", "charge", "chronic", "climate", "cluster", "college",
              "concept", "condition", "confirm", "contest", "control", "courage", "couple", "create", "current", "custom",
              "danger", "debate", "decade", "decide", "defend", "define", "degree", "demand", "dental", "desire", "detect",
              "device", "differ", "disease", "donate", "double", "driven", "duplex", "eagle", "earth", "editor", "employ",
              "enrich", "escape", "essence", "expand", "export", "fashion", "federal", "fiction", "finance", "fiscal",
              "follow", "force", "former", "gadget", "garden", "genuine", "glance", "global", "guitar", "handle", "height",
              "hiking", "honor", "human", "ideal", "impact", "income", "income", "inquire", "inspect", "integrate", "invite",
              "journal", "journey", "justice", "kitchen", "knight", "laptop", "launch", "lively", "limited", "listen",
              "lotion", "machine", "manual", "market", "mature", "member", "modern", "monitor", "motion", "mountain",
              "murder", "nature", "neither", "nimble", "noble", "notice", "obtain", "office", "online", "oppose", "option",
              "orange", "other", "origin", "output", "pencil", "people", "period", "piano", "planet", "police", "precise",
              "profit", "prompt", "proof", "public", "quality", "quarry", "rescue", "ribbon", "remark", "reply", "rescue",
              "respect", "safety", "season", "sponsor", "stable", "status", "stealth", "summer", "supply", "sustain",
              "system", "target", "tender", "theory", "thesis", "tiger", "tutor", "vacuum", "value", "vehicle", "vision",
              "vocal", "wallet", "wedding", "weight", "welfare", "window", "winter", "workshop", "yogurt", "zone", "zenith"
      );


      // Create and add documents
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();

        // Create a unique ID for each document
        String docId = "doc" + i;  // You can modify this to any format you prefer, such as using UUID.randomUUID()

        // Add the ID field to the document (make sure it is indexed if needed for searching)
        doc.add(new StringField("id", docId, Field.Store.NO));  // Use StringField for non-analyzed ID

        String randomContent = generateRandomContent(10000, words, random); // Generate random content with real words
        doc.add(new TextField("content", "Hej jag heter bob", Field.Store.NO));
       // doc.add(new TextField("content", "Hej jag heter bob", Field.Store.NO));

        writer.addDocument(doc);
        break;
      }

      // Commit changes and close writer
      writer.commit();
      writer.close();

      System.out.println(numDocs + " documents indexed.");
    } catch (IOException e) {
      e.printStackTrace();
    }
    Path indexPathPath = Paths.get(indexPath);
    // Calculate and print the size of the index
    long indexSize = getIndexSize(indexPathPath);
    System.out.println("Total size of the index: " + formatSize(indexSize));

    SearchTest(indexPath);
  }

  // Method to generate random content using real words with a fixed random seed
  private static String generateRandomContent(int length, List<String> words, Random random) {
    StringBuilder randomString = new StringBuilder();
    int wordCount = words.size();

    for (int i = 0; i < length; i++) { // Adjust the 5 based on average word length
      String word = words.get(random.nextInt(wordCount));
      randomString.append(word).append(" ");
    }

    return randomString.toString().trim();
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
    try {
      Directory directory = FSDirectory.open(Paths.get(indexPath));
      IndexReader reader = DirectoryReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(reader);

      QueryParser parser = new QueryParser("content", new StandardAnalyzer());
      try {
        Query query = parser.parse("alcohol");
        TopDocs topDocs = searcher.search(query, 10);
        System.out.println("Total hits: " + topDocs.totalHits.value);

        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
          System.out.println(doc.get("id"));
        }
      }  catch (ParseException e) {
        e.printStackTrace();
      }



      reader.close();
    }
  catch (IOException e) {
    e.printStackTrace();
  }

  }
}

