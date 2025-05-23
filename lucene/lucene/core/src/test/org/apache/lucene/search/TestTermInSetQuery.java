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
package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

public class TestTermInSetQuery extends LuceneTestCase {

  public void testAllDocsInFieldTerm() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    String field = "f";

    BytesRef denseTerm = new BytesRef(TestUtil.randomAnalysisString(random(), 10, true));

    Set<BytesRef> randomTerms = new HashSet<>();
    while (randomTerms.size()
        < AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
      randomTerms.add(new BytesRef(TestUtil.randomAnalysisString(random(), 10, true)));
    }
    assert randomTerms.size()
        == AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD;
    BytesRef[] otherTerms = new BytesRef[randomTerms.size()];
    int idx = 0;
    for (BytesRef term : randomTerms) {
      otherTerms[idx++] = term;
    }

    // Every doc with a value for `field` will contain `denseTerm`:
    int numDocs = 10 * otherTerms.length;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField(field, denseTerm, Store.NO));
      BytesRef sparseTerm = otherTerms[i % otherTerms.length];
      doc.add(new StringField(field, sparseTerm, Store.NO));
      iw.addDocument(doc);
    }

    // Make sure there are some docs in the index that don't contain a value for the field at all:
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
    }

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    List<BytesRef> queryTerms = Arrays.stream(otherTerms).collect(Collectors.toList());
    queryTerms.add(denseTerm);

    TermInSetQuery query = new TermInSetQuery(field, queryTerms);
    TopDocs topDocs = searcher.search(query, numDocs);
    assertEquals(numDocs, topDocs.totalHits.value);

    reader.close();
    dir.close();
  }

  public void testDuel() throws IOException {
    final int iters = atLeast(2);
    final String field = "f";
    for (int iter = 0; iter < iters; ++iter) {
      final List<BytesRef> allTerms = new ArrayList<>();
      final int numTerms = TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 10));
      for (int i = 0; i < numTerms; ++i) {
        final String value = TestUtil.randomAnalysisString(random(), 10, true);
        allTerms.add(newBytesRef(value));
      }
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final BytesRef term = allTerms.get(random().nextInt(allTerms.size()));
        doc.add(new StringField(field, term, Store.NO));
        iw.addDocument(doc);
      }
      if (numTerms > 1 && random().nextBoolean()) {
        iw.deleteDocuments(new TermQuery(new Term(field, allTerms.get(0))));
      }
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      if (reader.numDocs() == 0) {
        // may occasionally happen if all documents got the same term
        IOUtils.close(reader, dir);
        continue;
      }

      for (int i = 0; i < 100; ++i) {
        final float boost = random().nextFloat() * 10;
        final int numQueryTerms =
            TestUtil.nextInt(random(), 1, 1 << TestUtil.nextInt(random(), 1, 8));
        List<BytesRef> queryTerms = new ArrayList<>();
        for (int j = 0; j < numQueryTerms; ++j) {
          queryTerms.add(allTerms.get(random().nextInt(allTerms.size())));
        }
        final BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef t : queryTerms) {
          bq.add(new TermQuery(new Term(field, t)), Occur.SHOULD);
        }
        final Query q1 = new ConstantScoreQuery(bq.build());
        final Query q2 = new TermInSetQuery(field, queryTerms);
        assertSameMatches(searcher, new BoostQuery(q1, boost), new BoostQuery(q2, boost), true);
      }

      reader.close();
      dir.close();
    }
  }

  public void testReturnsNullScoreSupplier() throws Exception {
    try (Directory directory = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        for (char ch = 'a'; ch <= 'z'; ch++) {
          Document doc = new Document();
          doc.add(new KeywordField("id", Character.toString(ch), Field.Store.YES));
          doc.add(new KeywordField("content", Character.toString(ch), Field.Store.YES));
          writer.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        List<BytesRef> terms = new ArrayList<>();
        for (char ch = 'a'; ch <= 'z'; ch++) {
          terms.add(newBytesRef(Character.toString(ch)));
        }
        Query query2 = new TermInSetQuery("content", terms);

        {
          // query1 doesn't match any documents
          Query query1 = new TermInSetQuery("id", List.of(newBytesRef("aaa"), newBytesRef("bbb")));
          BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
          queryBuilder.add(query1, Occur.FILTER);
          queryBuilder.add(query2, Occur.FILTER);
          Query boolQuery = queryBuilder.build();

          IndexSearcher searcher = new IndexSearcher(reader);
          final LeafReaderContext ctx = reader.leaves().get(0);

          Weight weight1 = searcher.createWeight(searcher.rewrite(query1), ScoreMode.COMPLETE, 1);
          ScorerSupplier scorerSupplier1 = weight1.scorerSupplier(ctx);
          // as query1 doesn't match any documents, its scorerSupplier must be null
          assertNull(scorerSupplier1);
          Weight weight = searcher.createWeight(searcher.rewrite(boolQuery), ScoreMode.COMPLETE, 1);
          // scorerSupplier of a bool query where query1 is mandatory must be null
          ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
          assertNull(scorerSupplier);
        }
        {
          // query1 matches some documents
          Query query1 =
              new TermInSetQuery(
                  "id", List.of(newBytesRef("aaa"), newBytesRef("bbb"), newBytesRef("b")));
          BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
          queryBuilder.add(query1, Occur.FILTER);
          queryBuilder.add(query2, Occur.FILTER);
          Query boolQuery = queryBuilder.build();

          IndexSearcher searcher = new IndexSearcher(reader);
          final LeafReaderContext ctx = reader.leaves().get(0);

          Weight weight1 = searcher.createWeight(searcher.rewrite(query1), ScoreMode.COMPLETE, 1);
          ScorerSupplier scorerSupplier1 = weight1.scorerSupplier(ctx);
          // as query1 matches some documents, its scorerSupplier must not be null
          assertNotNull(scorerSupplier1);
          Weight weight = searcher.createWeight(searcher.rewrite(boolQuery), ScoreMode.COMPLETE, 1);
          // scorerSupplier of a bool query where query1 is mandatory must not be null
          ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
          assertNotNull(scorerSupplier);
        }
      }
    }
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores)
      throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits.value, td2.totalHits.value);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  public void testHashCodeAndEquals() {
    int num = atLeast(100);
    List<BytesRef> terms = new ArrayList<>();
    Set<BytesRef> uniqueTerms = new HashSet<>();
    for (int i = 0; i < num; i++) {
      String string = TestUtil.randomRealisticUnicodeString(random());
      terms.add(newBytesRef(string));
      uniqueTerms.add(newBytesRef(string));
      TermInSetQuery left = new TermInSetQuery("field", uniqueTerms);
      Collections.shuffle(terms, random());
      TermInSetQuery right = new TermInSetQuery("field", terms);
      assertEquals(right, left);
      assertEquals(right.hashCode(), left.hashCode());
      if (uniqueTerms.size() > 1) {
        List<BytesRef> asList = new ArrayList<>(uniqueTerms);
        asList.remove(0);
        TermInSetQuery notEqual = new TermInSetQuery("field", asList);
        assertFalse(left.equals(notEqual));
        assertFalse(right.equals(notEqual));
      }
    }

    TermInSetQuery tq1 = new TermInSetQuery("thing", List.of(newBytesRef("apple")));
    TermInSetQuery tq2 = new TermInSetQuery("thing", List.of(newBytesRef("orange")));
    assertFalse(tq1.hashCode() == tq2.hashCode());

    // different fields with the same term should have differing hashcodes
    tq1 = new TermInSetQuery("thing", List.of(newBytesRef("apple")));
    tq2 = new TermInSetQuery("thing2", List.of(newBytesRef("apple")));
    assertFalse(tq1.hashCode() == tq2.hashCode());
  }

  public void testSimpleEquals() {
    // Two terms with the same hash code
    assertEquals("AaAaBB".hashCode(), "BBBBBB".hashCode());
    TermInSetQuery left =
        new TermInSetQuery("id", List.of(newBytesRef("AaAaAa"), newBytesRef("AaAaBB")));
    TermInSetQuery right =
        new TermInSetQuery("id", List.of(newBytesRef("AaAaAa"), newBytesRef("BBBBBB")));
    assertFalse(left.equals(right));
  }

  public void testToString() {
    TermInSetQuery termsQuery =
        new TermInSetQuery("field1", List.of(newBytesRef("a"), newBytesRef("b"), newBytesRef("c")));
    assertEquals("field1:(a b c)", termsQuery.toString());
  }

  public void testDedup() {
    Query query1 = new TermInSetQuery("foo", List.of(newBytesRef("bar")));
    Query query2 = new TermInSetQuery("foo", List.of(newBytesRef("bar"), newBytesRef("bar")));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testOrderDoesNotMatter() {
    // order of terms if different
    Query query1 = new TermInSetQuery("foo", List.of(newBytesRef("bar"), newBytesRef("baz")));
    Query query2 = new TermInSetQuery("foo", List.of(newBytesRef("baz"), newBytesRef("bar")));
    QueryUtils.checkEqual(query1, query2);
  }

  public void testRamBytesUsed() {
    List<BytesRef> terms = new ArrayList<>();
    final int numTerms = 10000 + random().nextInt(1000);
    for (int i = 0; i < numTerms; ++i) {
      terms.add(newBytesRef(RandomStrings.randomUnicodeOfLength(random(), 10)));
    }
    TermInSetQuery query = new TermInSetQuery("f", terms);
    final long actualRamBytesUsed = RamUsageTester.ramUsed(query);
    final long expectedRamBytesUsed = query.ramBytesUsed();
    // error margin within 5%
    assertEquals(
        (double) expectedRamBytesUsed, (double) actualRamBytesUsed, actualRamBytesUsed / 20.d);
  }

  private static class TermsCountingDirectoryReaderWrapper extends FilterDirectoryReader {

    private final AtomicInteger counter;

    public TermsCountingDirectoryReaderWrapper(DirectoryReader in, AtomicInteger counter)
        throws IOException {
      super(in, new TermsCountingSubReaderWrapper(counter));
      this.counter = counter;
    }

    private static class TermsCountingSubReaderWrapper extends SubReaderWrapper {
      private final AtomicInteger counter;

      public TermsCountingSubReaderWrapper(AtomicInteger counter) {
        this.counter = counter;
      }

      @Override
      public LeafReader wrap(LeafReader reader) {
        return new TermsCountingLeafReaderWrapper(reader, counter);
      }
    }

    private static class TermsCountingLeafReaderWrapper extends FilterLeafReader {

      private final AtomicInteger counter;

      public TermsCountingLeafReaderWrapper(LeafReader in, AtomicInteger counter) {
        super(in);
        this.counter = counter;
      }

      @Override
      public Terms terms(String field) throws IOException {
        Terms terms = super.terms(field);
        if (terms == null) {
          return null;
        }
        return new FilterTerms(terms) {
          @Override
          public TermsEnum iterator() throws IOException {
            counter.incrementAndGet();
            return super.iterator();
          }
        };
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new TermsCountingDirectoryReaderWrapper(in, counter);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  public void testPullOneTermsEnum() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "1", Store.NO));
    w.addDocument(doc);
    DirectoryReader reader = w.getReader();
    w.close();
    final AtomicInteger counter = new AtomicInteger();
    DirectoryReader wrapped = new TermsCountingDirectoryReaderWrapper(reader, counter);

    final List<BytesRef> terms = new ArrayList<>();
    // enough terms to avoid the rewrite
    final int numTerms =
        TestUtil.nextInt(
            random(),
            AbstractMultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD + 1,
            100);
    for (int i = 0; i < numTerms; ++i) {
      final BytesRef term = newBytesRef(RandomStrings.randomUnicodeOfCodepointLength(random(), 10));
      terms.add(term);
    }

    assertEquals(0, new IndexSearcher(wrapped).count(new TermInSetQuery("bar", terms)));
    assertEquals(0, counter.get()); // missing field
    new IndexSearcher(wrapped).count(new TermInSetQuery("foo", terms));
    assertEquals(1, counter.get());
    wrapped.close();
    dir.close();
  }

  public void testBinaryToString() {
    TermInSetQuery query =
        new TermInSetQuery("field", List.of(newBytesRef(new byte[] {(byte) 0xff, (byte) 0xfe})));
    assertEquals("field:([ff fe])", query.toString());
  }

  public void testIsConsideredCostlyByQueryCache() throws IOException {
    TermInSetQuery query =
        new TermInSetQuery("foo", List.of(newBytesRef("bar"), newBytesRef("baz")));
    UsageTrackingQueryCachingPolicy policy = new UsageTrackingQueryCachingPolicy();
    assertFalse(policy.shouldCache(query));
    policy.onUse(query);
    policy.onUse(query);
    // cached after two uses
    assertTrue(policy.shouldCache(query));
  }

  public void testVisitor() {
    // singleton reports back to consumeTerms()
    TermInSetQuery singleton = new TermInSetQuery("field", List.of(newBytesRef("term1")));
    singleton.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            assertEquals(1, terms.length);
            assertEquals(new Term("field", newBytesRef("term1")), terms[0]);
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            fail("Singleton TermInSetQuery should not try to build ByteRunAutomaton");
          }
        });

    // multiple values built into automaton
    List<BytesRef> terms = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      terms.add(newBytesRef("term" + i));
    }
    TermInSetQuery t = new TermInSetQuery("field", terms);
    t.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            fail("TermInSetQuery with multiple terms should build automaton");
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            ByteRunAutomaton a = automaton.get();
            BytesRef test = newBytesRef("nonmatching");
            assertFalse(a.run(test.bytes, test.offset, test.length));
            for (BytesRef term : terms) {
              assertTrue(a.run(term.bytes, term.offset, term.length));
            }
          }
        });
  }

  public void testTermsIterator() throws IOException {
    TermInSetQuery empty = new TermInSetQuery("field", Collections.emptyList());
    BytesRefIterator it = empty.getBytesRefIterator();
    assertNull(it.next());

    TermInSetQuery query =
        new TermInSetQuery(
            "field", List.of(newBytesRef("term1"), newBytesRef("term2"), newBytesRef("term3")));
    it = query.getBytesRefIterator();
    assertEquals(newBytesRef("term1"), it.next());
    assertEquals(newBytesRef("term2"), it.next());
    assertEquals(newBytesRef("term3"), it.next());
    assertNull(it.next());
  }
}
