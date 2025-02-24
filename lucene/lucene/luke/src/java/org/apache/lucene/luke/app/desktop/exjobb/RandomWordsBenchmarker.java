package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

public class RandomWordsBenchmarker implements DatasetCompressionBenchmarker {
    @Override
    public String GetDatasetName() {
        return "RandomWords";
    }

    @Override
    public IndexingBenchmarkData BenchmarkIndexing(IndexWriter writer) throws IOException {

        int numDocs = 1000; // Number of documents to index
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

        long start = System.currentTimeMillis();
        // Create and add documents
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();

            // Create a unique ID for each document
            String docId = "doc" + i;  // You can modify this to any format you prefer, such as using UUID.randomUUID()

            // Add the ID field to the document (make sure it is indexed if needed for searching)
            doc.add(new StringField("id", docId, Field.Store.NO));  // Use StringField for non-analyzed ID

            FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
            fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);


            String randomContent = generateRandomContent(10000, words, random); // Generate random content with real words
            doc.add(new TextField("content", randomContent, Field.Store.NO));


            writer.addDocument(doc);
        }

        // Commit changes and close writer
        writer.commit();
        writer.close();

        System.out.println(numDocs + " documents indexed.");

        long elapsedTime = System.currentTimeMillis() - start;

        System.out.println("Indexing time in Milli seconds: " + elapsedTime);

        IndexingBenchmarkData result = new IndexingBenchmarkData();
        result.totalIndexingTimeInMS = elapsedTime;
        result.totalIndexSizeInMB = BenchmarkUtils.getIndexSizeInMB("index");

        return result;
    }

    @Override
    public SearchBenchmarkData BenchmarkSearching(String indexPath) {
        try {

            Directory directory = FSDirectory.open(Paths.get(indexPath));
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            QueryParser parser = new QueryParser("content", new StandardAnalyzer());
            try {
                Query query = parser.parse("noble mature");

                // Create a phrase query for the phrase "quick brown fox"
                PhraseQuery.Builder builder = new PhraseQuery.Builder();
                builder.add(new Term("content", "decide"), 0); // "quick" at position 0
                builder.add(new Term("content", "alcohol"), 1); // "brown" at position 1
                // builder.add(new Term("content", "define"), 2);   // "fox" at position 2
                PhraseQuery phraseQuery = builder.build();

                TopDocs topDocs = searcher.search(phraseQuery, 10);
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

        return null;
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

}
