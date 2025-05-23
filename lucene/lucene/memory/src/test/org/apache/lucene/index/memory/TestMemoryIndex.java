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
package org.apache.lucene.index.memory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.StringContains.containsString;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.LongStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockPayloadAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryIndex extends LuceneTestCase {

  private MockAnalyzer analyzer;

  @Before
  public void setup() {
    analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false); // MemoryIndex can close a TokenStream on init error
  }

  @Test
  public void testFreezeAPI() {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "some text", analyzer);

    assertThat(mi.search(new MatchAllDocsQuery()), not(is(0.0f)));
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    // check we can add a new field after searching
    mi.addField("f2", "some more text", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f2", "some"))), not(is(0.0f)));

    // freeze!
    mi.freeze();

    RuntimeException expected =
        expectThrows(
            RuntimeException.class,
            () -> {
              mi.addField("f3", "and yet more", analyzer);
            });
    assertThat(expected.getMessage(), containsString("frozen"));

    expected =
        expectThrows(
            RuntimeException.class,
            () -> {
              mi.setSimilarity(new BM25Similarity(1, 1));
            });
    assertThat(expected.getMessage(), containsString("frozen"));

    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    mi.reset();
    mi.addField("f1", "wibble", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("f1", "wibble"))), not(is(0.0f)));

    // check we can set the Similarity again
    mi.setSimilarity(new ClassicSimilarity());
  }

  public void testSeekByTermOrd() throws IOException {
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    TermsEnum terms = reader.terms("field").iterator();
    terms.seekExact(0);
    assertEquals("be", terms.term().utf8ToString());
    TestUtil.checkReader(reader);
  }

  public void testFieldsOnlyReturnsIndexedFields() throws IOException {
    Document doc = new Document();

    doc.add(new NumericDocValuesField("numeric", 29L));
    doc.add(new TextField("text", "some text", Field.Store.NO));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher searcher = mi.createSearcher();
    IndexReader reader = searcher.getIndexReader();

    assertEquals(reader.termVectors().get(0).size(), 1);
  }

  public void testReaderConsistency() throws IOException {
    Analyzer analyzer = new MockPayloadAnalyzer();

    // defaults
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());

    // all combinations of offsets/payloads options
    mi = new MemoryIndex(true, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());

    mi = new MemoryIndex(true, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());

    mi = new MemoryIndex(false, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());

    mi = new MemoryIndex(false, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());

    analyzer.close();
  }

  @Test
  public void testSimilarities() throws IOException {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "a long text field that contains many many terms", analyzer);

    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    NumericDocValues norms = reader.getNormValues("f1");
    assertEquals(0, norms.nextDoc());
    float n1 = norms.longValue();

    // Norms are re-computed when we change the Similarity
    mi.setSimilarity(
        new Similarity() {

          @Override
          public long computeNorm(FieldInvertState state) {
            return 74;
          }

          @Override
          public SimScorer scorer(
              float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            throw new UnsupportedOperationException();
          }
        });
    norms = reader.getNormValues("f1");
    assertEquals(0, norms.nextDoc());
    float n2 = norms.longValue();

    assertTrue(n1 != n2);
    TestUtil.checkReader(reader);
  }

  @Test
  public void testOmitNorms() throws IOException {
    MemoryIndex mi = new MemoryIndex();
    FieldType ft = new FieldType();
    ft.setTokenized(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setOmitNorms(true);
    mi.addField(new Field("f1", "some text in here", ft), analyzer);
    mi.freeze();

    LeafReader leader = (LeafReader) mi.createSearcher().getIndexReader();
    NumericDocValues norms = leader.getNormValues("f1");
    assertNull(norms);
  }

  @Test
  public void testBuildFromDocument() {

    Document doc = new Document();
    doc.add(new TextField("field1", "some text", Field.Store.NO));
    doc.add(new TextField("field1", "some more text", Field.Store.NO));
    doc.add(new StringField("field2", "untokenized text", Field.Store.NO));

    analyzer.setPositionIncrementGap(100);

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);

    assertThat(mi.search(new TermQuery(new Term("field1", "text"))), not(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "text"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "untokenized text"))), not(0.0f));

    assertThat(mi.search(new TermQuery(new Term("field1", "some more text"))), is(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "some", "more", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "some", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "text", "some")), is(0.0f));
  }

  public void testDocValues() throws Exception {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("numeric", 29L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 33L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 31L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 30L));
    doc.add(new BinaryDocValuesField("binary", new BytesRef("a")));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("f")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("c")));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    NumericDocValues numericDocValues = leafReader.getNumericDocValues("numeric");
    assertEquals(0, numericDocValues.nextDoc());
    assertEquals(29L, numericDocValues.longValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, numericDocValues.nextDoc());
    SortedNumericDocValues sortedNumericDocValues =
        leafReader.getSortedNumericDocValues("sorted_numeric");
    assertEquals(0, sortedNumericDocValues.nextDoc());
    assertEquals(5, sortedNumericDocValues.docValueCount());
    assertEquals(30L, sortedNumericDocValues.nextValue());
    assertEquals(31L, sortedNumericDocValues.nextValue());
    assertEquals(32L, sortedNumericDocValues.nextValue());
    assertEquals(32L, sortedNumericDocValues.nextValue());
    assertEquals(33L, sortedNumericDocValues.nextValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sortedNumericDocValues.nextDoc());
    BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("binary");
    assertEquals(0, binaryDocValues.nextDoc());
    assertEquals("a", binaryDocValues.binaryValue().utf8ToString());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, binaryDocValues.nextDoc());
    SortedDocValues sortedDocValues = leafReader.getSortedDocValues("sorted");
    assertEquals(0, sortedDocValues.nextDoc());
    assertEquals("b", sortedDocValues.lookupOrd(sortedDocValues.ordValue()).utf8ToString());
    assertEquals(0, sortedDocValues.ordValue());
    assertEquals("b", sortedDocValues.lookupOrd(0).utf8ToString());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sortedDocValues.nextDoc());
    SortedSetDocValues sortedSetDocValues = leafReader.getSortedSetDocValues("sorted_set");
    assertEquals(3, sortedSetDocValues.getValueCount());
    assertEquals(0, sortedSetDocValues.nextDoc());
    assertEquals(3, sortedSetDocValues.docValueCount());
    assertEquals(0L, sortedSetDocValues.nextOrd());
    assertEquals(1L, sortedSetDocValues.nextOrd());
    assertEquals(2L, sortedSetDocValues.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSetDocValues.nextOrd());
    assertEquals("c", sortedSetDocValues.lookupOrd(0L).utf8ToString());
    assertEquals("d", sortedSetDocValues.lookupOrd(1L).utf8ToString());
    assertEquals("f", sortedSetDocValues.lookupOrd(2L).utf8ToString());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, sortedDocValues.nextDoc());
  }

  public void testDocValues_resetIterator() throws Exception {
    Document doc = new Document();

    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("f")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("c")));

    doc.add(new SortedNumericDocValuesField("sorted_numeric", 33L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 31L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 30L));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();

    SortedSetDocValues sortedSetDocValues = leafReader.getSortedSetDocValues("sorted_set");
    assertEquals(3, sortedSetDocValues.getValueCount());
    for (int times = 0; times < 3; times++) {
      assertTrue(sortedSetDocValues.advanceExact(0));
      assertEquals(3, sortedSetDocValues.docValueCount());
      assertEquals(0L, sortedSetDocValues.nextOrd());
      assertEquals(1L, sortedSetDocValues.nextOrd());
      assertEquals(2L, sortedSetDocValues.nextOrd());
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSetDocValues.nextOrd());
    }

    SortedNumericDocValues sortedNumericDocValues =
        leafReader.getSortedNumericDocValues("sorted_numeric");
    for (int times = 0; times < 3; times++) {
      assertTrue(sortedNumericDocValues.advanceExact(0));
      assertEquals(5, sortedNumericDocValues.docValueCount());
      assertEquals(30L, sortedNumericDocValues.nextValue());
      assertEquals(31L, sortedNumericDocValues.nextValue());
      assertEquals(32L, sortedNumericDocValues.nextValue());
      assertEquals(32L, sortedNumericDocValues.nextValue());
      assertEquals(33L, sortedNumericDocValues.nextValue());
    }
  }

  public void testInvalidDocValuesUsage() throws Exception {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", 29L));
    doc.add(new BinaryDocValuesField("field", new BytesRef("30")));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "cannot change DocValues type from NUMERIC to BINARY for field \"field\"",
          e.getMessage());
    }

    doc = new Document();
    doc.add(new NumericDocValuesField("field", 29L));
    doc.add(new NumericDocValuesField("field", 30L));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Only one value per field allowed for [NUMERIC] doc values field [field]",
          e.getMessage());
    }

    doc = new Document();
    doc.add(new TextField("field", "a b", Field.Store.NO));
    doc.add(new BinaryDocValuesField("field", new BytesRef("a")));
    doc.add(new BinaryDocValuesField("field", new BytesRef("b")));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Only one value per field allowed for [BINARY] doc values field [field]", e.getMessage());
    }

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("a")));
    doc.add(new SortedDocValuesField("field", new BytesRef("b")));
    doc.add(new TextField("field", "a b", Field.Store.NO));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Only one value per field allowed for [SORTED] doc values field [field]", e.getMessage());
    }
  }

  public void testDocValuesDoNotAffectBoostPositionsOrOffset() throws Exception {
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("text", new BytesRef("quick brown fox")));
    doc.add(new TextField("text", "quick brown fox", Field.Store.NO));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer, true, true);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    TermsEnum tenum = leafReader.terms("text").iterator();

    assertEquals("brown", tenum.next().utf8ToString());
    PostingsEnum penum = tenum.postings(null, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(1, penum.nextPosition());
    assertEquals(6, penum.startOffset());
    assertEquals(11, penum.endOffset());

    assertEquals("fox", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(2, penum.nextPosition());
    assertEquals(12, penum.startOffset());
    assertEquals(15, penum.endOffset());

    assertEquals("quick", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(0, penum.nextPosition());
    assertEquals(0, penum.startOffset());
    assertEquals(5, penum.endOffset());

    BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("text");
    assertEquals(0, binaryDocValues.nextDoc());
    assertEquals("quick brown fox", binaryDocValues.binaryValue().utf8ToString());
  }

  public void testBigBinaryDocValues() throws Exception {
    Document doc = new Document();
    byte[] bytes = new byte[33 * 1024];
    random().nextBytes(bytes);
    doc.add(new BinaryDocValuesField("binary", new BytesRef(bytes)));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer, true, true);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("binary");
    assertEquals(0, binaryDocValues.nextDoc());
    assertArrayEquals(bytes, binaryDocValues.binaryValue().bytes);
  }

  public void testBigSortedDocValues() throws Exception {
    Document doc = new Document();
    byte[] bytes = new byte[33 * 1024];
    random().nextBytes(bytes);
    doc.add(new SortedDocValuesField("binary", new BytesRef(bytes)));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer, true, true);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    SortedDocValues sortedDocValues = leafReader.getSortedDocValues("binary");
    assertEquals(0, sortedDocValues.nextDoc());
    assertArrayEquals(bytes, sortedDocValues.lookupOrd(0).bytes);
  }

  public void testPointValues() throws Exception {
    List<Function<Long, IndexableField>> fieldFunctions =
        Arrays.asList(
            (t) -> new IntPoint("number", t.intValue()),
            (t) -> new LongPoint("number", t),
            (t) -> new FloatPoint("number", t.floatValue()),
            (t) -> new DoublePoint("number", t.doubleValue()));
    List<Function<Long, Query>> exactQueryFunctions =
        Arrays.asList(
            (t) -> IntPoint.newExactQuery("number", t.intValue()),
            (t) -> LongPoint.newExactQuery("number", t),
            (t) -> FloatPoint.newExactQuery("number", t.floatValue()),
            (t) -> DoublePoint.newExactQuery("number", t.doubleValue()));
    List<Function<long[], Query>> setQueryFunctions =
        Arrays.asList(
            (t) ->
                IntPoint.newSetQuery(
                    "number", LongStream.of(t).mapToInt(value -> (int) value).toArray()),
            (t) -> LongPoint.newSetQuery("number", t),
            (t) ->
                FloatPoint.newSetQuery(
                    "number",
                    Arrays.asList(
                        LongStream.of(t).mapToObj(value -> (float) value).toArray(Float[]::new))),
            (t) ->
                DoublePoint.newSetQuery(
                    "number", LongStream.of(t).mapToDouble(value -> (double) value).toArray()));
    List<BiFunction<Long, Long, Query>> rangeQueryFunctions =
        Arrays.asList(
            (t, u) -> IntPoint.newRangeQuery("number", t.intValue(), u.intValue()),
            (t, u) -> LongPoint.newRangeQuery("number", t, u),
            (t, u) -> FloatPoint.newRangeQuery("number", t.floatValue(), u.floatValue()),
            (t, u) -> DoublePoint.newRangeQuery("number", t.doubleValue(), u.doubleValue()));

    for (int i = 0; i < fieldFunctions.size(); i++) {
      Function<Long, IndexableField> fieldFunction = fieldFunctions.get(i);
      Function<Long, Query> exactQueryFunction = exactQueryFunctions.get(i);
      Function<long[], Query> setQueryFunction = setQueryFunctions.get(i);
      BiFunction<Long, Long, Query> rangeQueryFunction = rangeQueryFunctions.get(i);

      Document doc = new Document();
      for (int number = 1; number < 32; number += 2) {
        doc.add(fieldFunction.apply((long) number));
      }
      MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
      IndexSearcher indexSearcher = mi.createSearcher();
      Query query = exactQueryFunction.apply(5L);
      assertEquals(1, indexSearcher.count(query));
      query = exactQueryFunction.apply(4L);
      assertEquals(0, indexSearcher.count(query));

      query = setQueryFunction.apply(new long[] {3L, 9L, 19L});
      assertEquals(1, indexSearcher.count(query));
      query = setQueryFunction.apply(new long[] {2L, 8L, 13L});
      assertEquals(1, indexSearcher.count(query));
      query = setQueryFunction.apply(new long[] {2L, 8L, 16L});
      assertEquals(0, indexSearcher.count(query));

      query = rangeQueryFunction.apply(2L, 16L);
      assertEquals(1, indexSearcher.count(query));
      query = rangeQueryFunction.apply(24L, 48L);
      assertEquals(1, indexSearcher.count(query));
      query = rangeQueryFunction.apply(48L, 68L);
      assertEquals(0, indexSearcher.count(query));
    }
  }

  public void testMissingPoints() throws IOException {
    Document doc = new Document();
    doc.add(new StoredField("field", 42));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher indexSearcher = mi.createSearcher();
    // field that exists but does not have points
    assertNull(indexSearcher.getIndexReader().leaves().get(0).reader().getPointValues("field"));
    // field that does not exist
    assertNull(
        indexSearcher
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getPointValues("some_missing_field"));
  }

  public void testPointValuesDoNotAffectPositionsOrOffset() throws Exception {
    MemoryIndex mi = new MemoryIndex(true, true);
    mi.addField(new TextField("text", "quick brown fox", Field.Store.NO), analyzer);
    mi.addField(new BinaryPoint("text", "quick".getBytes(StandardCharsets.UTF_8)), analyzer);
    mi.addField(new BinaryPoint("text", "brown".getBytes(StandardCharsets.UTF_8)), analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    TermsEnum tenum = leafReader.terms("text").iterator();

    assertEquals("brown", tenum.next().utf8ToString());
    PostingsEnum penum = tenum.postings(null, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(1, penum.nextPosition());
    assertEquals(6, penum.startOffset());
    assertEquals(11, penum.endOffset());

    assertEquals("fox", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(2, penum.nextPosition());
    assertEquals(12, penum.startOffset());
    assertEquals(15, penum.endOffset());

    assertEquals("quick", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(0, penum.nextPosition());
    assertEquals(0, penum.startOffset());
    assertEquals(5, penum.endOffset());

    IndexSearcher indexSearcher = mi.createSearcher();
    assertEquals(
        1,
        indexSearcher.count(
            BinaryPoint.newExactQuery("text", "quick".getBytes(StandardCharsets.UTF_8))));
    assertEquals(
        1,
        indexSearcher.count(
            BinaryPoint.newExactQuery("text", "brown".getBytes(StandardCharsets.UTF_8))));
    assertEquals(
        0,
        indexSearcher.count(
            BinaryPoint.newExactQuery("text", "jumps".getBytes(StandardCharsets.UTF_8))));
  }

  public void test2DPoints() throws Exception {
    Document doc = new Document();
    doc.add(new IntPoint("ints", 0, -100));
    doc.add(new IntPoint("ints", 20, 20));
    doc.add(new IntPoint("ints", 100, -100));
    doc.add(new LongPoint("longs", 0L, -100L));
    doc.add(new LongPoint("longs", 20L, 20L));
    doc.add(new LongPoint("longs", 100L, -100L));
    doc.add(new FloatPoint("floats", 0F, -100F));
    doc.add(new FloatPoint("floats", 20F, 20F));
    doc.add(new FloatPoint("floats", 100F, -100F));
    doc.add(new DoublePoint("doubles", 0D, -100D));
    doc.add(new DoublePoint("doubles", 20D, 20D));
    doc.add(new DoublePoint("doubles", 100D, -100D));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher s = mi.createSearcher();

    assertEquals(
        1, s.count(IntPoint.newRangeQuery("ints", new int[] {10, 10}, new int[] {30, 30})));
    assertEquals(
        1, s.count(LongPoint.newRangeQuery("longs", new long[] {10L, 10L}, new long[] {30L, 30L})));
    assertEquals(
        1,
        s.count(
            FloatPoint.newRangeQuery("floats", new float[] {10F, 10F}, new float[] {30F, 30F})));
    assertEquals(
        1,
        s.count(
            DoublePoint.newRangeQuery(
                "doubles", new double[] {10D, 10D}, new double[] {30D, 30D})));
  }

  public void testMultiValuedPointsSortedCorrectly() throws Exception {
    Document doc = new Document();
    doc.add(new IntPoint("ints", 3));
    doc.add(new IntPoint("ints", 2));
    doc.add(new IntPoint("ints", 1));
    doc.add(new LongPoint("longs", 3L));
    doc.add(new LongPoint("longs", 2L));
    doc.add(new LongPoint("longs", 1L));
    doc.add(new FloatPoint("floats", 3F));
    doc.add(new FloatPoint("floats", 2F));
    doc.add(new FloatPoint("floats", 1F));
    doc.add(new DoublePoint("doubles", 3D));
    doc.add(new DoublePoint("doubles", 2D));
    doc.add(new DoublePoint("doubles", 1D));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher s = mi.createSearcher();

    assertEquals(1, s.count(IntPoint.newSetQuery("ints", 2)));
    assertEquals(1, s.count(LongPoint.newSetQuery("longs", 2)));
    assertEquals(1, s.count(FloatPoint.newSetQuery("floats", 2)));
    assertEquals(1, s.count(DoublePoint.newSetQuery("doubles", 2)));
  }

  public void testIndexingPointsAndDocValues() throws Exception {
    FieldType type = new FieldType();
    type.setDimensions(1, 4);
    type.setDocValuesType(DocValuesType.BINARY);
    type.freeze();
    Document doc = new Document();
    byte[] packedPoint = "term".getBytes(StandardCharsets.UTF_8);
    doc.add(new BinaryPoint("field", packedPoint, type));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();

    assertEquals(1, leafReader.getPointValues("field").size());
    assertArrayEquals(packedPoint, leafReader.getPointValues("field").getMinPackedValue());
    assertArrayEquals(packedPoint, leafReader.getPointValues("field").getMaxPackedValue());

    BinaryDocValues dvs = leafReader.getBinaryDocValues("field");
    assertEquals(0, dvs.nextDoc());
    assertEquals("term", dvs.binaryValue().utf8ToString());
  }

  public void testToStringDebug() {
    MemoryIndex mi = new MemoryIndex(true, true);
    Analyzer analyzer = new MockPayloadAnalyzer();

    mi.addField("analyzedField", "aa bb aa", analyzer);

    FieldType type = new FieldType();
    type.setDimensions(1, 4);
    type.setDocValuesType(DocValuesType.BINARY);
    type.freeze();
    mi.addField(
        new BinaryPoint("pointAndDvField", "term".getBytes(StandardCharsets.UTF_8), type),
        analyzer);

    assertEquals(
        "analyzedField:\n"
            + "\t'[61 61]':2: [(0, 0, 2, [70 6f 73 3a 20 30]), (1, 6, 8, [70 6f 73 3a 20 32])]\n"
            + "\t'[62 62]':1: [(1, 3, 5, [70 6f 73 3a 20 31])]\n"
            + "\tterms=2, positions=3\n"
            + "pointAndDvField:\n"
            + "\tterms=0, positions=0\n"
            + "\n"
            + "fields=2, terms=2, positions=3",
        mi.toStringDebug());
  }

  public void testStoredFields() throws IOException {
    List<IndexableField> fields = new ArrayList<>();
    fields.add(new StoredField("float", 1.5f));
    fields.add(new StoredField("multifloat", 2.5f));
    fields.add(new StoredField("multifloat", 3.5f));
    fields.add(new StoredField("long", 10L));
    fields.add(new StoredField("multilong", 15L));
    fields.add(new StoredField("multilong", 20L));
    fields.add(new StoredField("int", 1.7));
    fields.add(new StoredField("multiint", 2.7));
    fields.add(new StoredField("multiint", 2.8));
    fields.add(new StoredField("multiint", 2.9));
    fields.add(new StoredField("double", 3.7d));
    fields.add(new StoredField("multidouble", 4.5d));
    fields.add(new StoredField("multidouble", 4.6d));
    fields.add(new StoredField("multidouble", 4.7d));
    fields.add(new StoredField("string", "foo"));
    fields.add(new StoredField("multistring", "bar"));
    fields.add(new StoredField("multistring", "baz"));
    fields.add(new StoredField("binary", "bfoo".getBytes(StandardCharsets.UTF_8)));
    fields.add(new StoredField("multibinary", "bbar".getBytes(StandardCharsets.UTF_8)));
    fields.add(new StoredField("multibinary", "bbaz".getBytes(StandardCharsets.UTF_8)));

    Collections.shuffle(fields, random());
    Document doc = new Document();
    for (IndexableField f : fields) {
      doc.add(f);
    }

    MemoryIndex mi = MemoryIndex.fromDocument(doc, new StandardAnalyzer());
    Document d = mi.createSearcher().storedFields().document(0);

    assertContains(d, "long", 10L, IndexableField::numericValue);
    assertContains(d, "int", 1.7, IndexableField::numericValue);
    assertContains(d, "double", 3.7d, IndexableField::numericValue);
    assertContains(d, "float", 1.5f, IndexableField::numericValue);
    assertContains(d, "string", "foo", IndexableField::stringValue);
    assertBinaryContains(d, "binary", new BytesRef("bfoo"));

    assertMultiContains(d, "multilong", new Object[] {15L, 20L}, IndexableField::numericValue);
    assertMultiContains(d, "multiint", new Object[] {2.7, 2.8, 2.9}, IndexableField::numericValue);
    assertMultiContains(
        d, "multidouble", new Object[] {4.5d, 4.6d, 4.7d}, IndexableField::numericValue);
    assertMultiContains(d, "multifloat", new Object[] {2.5f, 3.5f}, IndexableField::numericValue);
    assertMultiContains(d, "multistring", new Object[] {"bar", "baz"}, IndexableField::stringValue);
    assertBinaryMultiContains(
        d, "multibinary", new BytesRef[] {new BytesRef("bbar"), new BytesRef("bbaz")});
  }

  public void testKnnFloatVectorOnlyOneVectorAllowed() throws IOException {
    Document doc = new Document();
    doc.add(new KnnFloatVectorField("knnFloatA", new float[] {1.0f, 2.0f}));
    doc.add(new KnnFloatVectorField("knnFloatA", new float[] {3.0f, 4.0f}));
    expectThrows(
        IllegalArgumentException.class,
        () -> MemoryIndex.fromDocument(doc, new StandardAnalyzer()));
  }

  public void testKnnFloatVectors() throws IOException {
    List<IndexableField> fields = new ArrayList<>();
    fields.add(new KnnFloatVectorField("knnFloatA", new float[] {1.0f, 2.0f}));
    fields.add(new KnnFloatVectorField("knnFloatB", new float[] {3.0f, 4.0f, 5.0f, 6.0f}));
    fields.add(
        new KnnFloatVectorField(
            "knnFloatC", new float[] {7.0f, 8.0f, 9.0f}, VectorSimilarityFunction.DOT_PRODUCT));
    Collections.shuffle(fields, random());
    Document doc = new Document();
    for (IndexableField f : fields) {
      doc.add(f);
    }

    MemoryIndex mi = MemoryIndex.fromDocument(doc, new StandardAnalyzer());
    assertFloatVectorValue(mi, "knnFloatA", new float[] {1.0f, 2.0f});
    assertFloatVectorValue(mi, "knnFloatB", new float[] {3.0f, 4.0f, 5.0f, 6.0f});
    assertFloatVectorValue(mi, "knnFloatC", new float[] {7.0f, 8.0f, 9.0f});

    assertFloatVectorScore(mi, "knnFloatA", new float[] {1.0f, 1.0f}, 0.5f);
    assertFloatVectorScore(mi, "knnFloatB", new float[] {3.0f, 3.0f, 3.0f, 3.0f}, 0.06666667f);
    assertFloatVectorScore(mi, "knnFloatC", new float[] {7.0f, 7.0f, 7.0f}, 84.5f);

    assertNull(
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getFloatVectorValues("knnFloatMissing"));
    assertNull(
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getByteVectorValues("knnByteVectorValue"));
  }

  public void testKnnByteVectorOnlyOneVectorAllowed() throws IOException {
    Document doc = new Document();
    doc.add(new KnnByteVectorField("knnByteA", new byte[] {1, 2}));
    doc.add(new KnnByteVectorField("knnByteA", new byte[] {3, 4}));
    expectThrows(
        IllegalArgumentException.class,
        () -> MemoryIndex.fromDocument(doc, new StandardAnalyzer()));
  }

  public void testKnnByteVectors() throws IOException {
    List<IndexableField> fields = new ArrayList<>();
    fields.add(new KnnByteVectorField("knnByteA", new byte[] {1, 2}));
    fields.add(new KnnByteVectorField("knnByteB", new byte[] {3, 4, 5, 6}));
    fields.add(
        new KnnByteVectorField(
            "knnByteC", new byte[] {7, 8, 9}, VectorSimilarityFunction.DOT_PRODUCT));
    Collections.shuffle(fields, random());
    Document doc = new Document();
    for (IndexableField f : fields) {
      doc.add(f);
    }

    MemoryIndex mi = MemoryIndex.fromDocument(doc, new StandardAnalyzer());
    assertByteVectorValue(mi, "knnByteA", new byte[] {1, 2});
    assertByteVectorValue(mi, "knnByteB", new byte[] {3, 4, 5, 6});
    assertByteVectorValue(mi, "knnByteC", new byte[] {7, 8, 9});

    assertByteVectorScore(mi, "knnByteA", new byte[] {1, 1}, 0.5f);
    assertByteVectorScore(mi, "knnByteB", new byte[] {3, 3, 3, 3}, 0.06666667f);
    assertByteVectorScore(mi, "knnByteC", new byte[] {7, 7, 7}, 0.501709f);

    assertNull(
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getByteVectorValues("knnByteMissing"));
    assertNull(
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getFloatVectorValues("knnFloatVectorValue"));
  }

  private static void assertFloatVectorValue(MemoryIndex mi, String fieldName, float[] expected)
      throws IOException {
    FloatVectorValues fvv =
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getFloatVectorValues(fieldName);
    assertNotNull(fvv);
    assertEquals(0, fvv.nextDoc());
    assertArrayEquals(expected, fvv.vectorValue(), 1e-6f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, fvv.nextDoc());
  }

  private static void assertFloatVectorScore(
      MemoryIndex mi, String fieldName, float[] queryVector, float expectedScore)
      throws IOException {
    FloatVectorValues fvv =
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getFloatVectorValues(fieldName);
    assertNotNull(fvv);
    if (random().nextBoolean()) {
      fvv.nextDoc();
    }
    VectorScorer scorer = fvv.scorer(queryVector);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(expectedScore, scorer.score(), 0.0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
  }

  private static void assertByteVectorValue(MemoryIndex mi, String fieldName, byte[] expected)
      throws IOException {
    ByteVectorValues bvv =
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getByteVectorValues(fieldName);
    assertNotNull(bvv);
    assertEquals(0, bvv.nextDoc());
    assertArrayEquals(expected, bvv.vectorValue());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, bvv.nextDoc());
  }

  private static void assertByteVectorScore(
      MemoryIndex mi, String fieldName, byte[] queryVector, float expectedScore)
      throws IOException {
    ByteVectorValues bvv =
        mi.createSearcher()
            .getIndexReader()
            .leaves()
            .get(0)
            .reader()
            .getByteVectorValues(fieldName);
    assertNotNull(bvv);
    if (random().nextBoolean()) {
      bvv.nextDoc();
    }
    VectorScorer scorer = bvv.scorer(queryVector);
    assertEquals(0, scorer.iterator().nextDoc());
    assertEquals(expectedScore, scorer.score(), 0.0f);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
  }

  private static void assertContains(
      Document d, String field, Object expected, Function<IndexableField, Object> value) {
    assertNotNull(d.getField(field));
    IndexableField[] fields = d.getFields(field);
    assertEquals(1, fields.length);
    assertEquals(expected, value.apply(fields[0]));
  }

  private static void assertBinaryContains(Document d, String field, BytesRef expected) {
    assertNotNull(d.getField(field));
    IndexableField[] fields = d.getFields(field);
    assertEquals(1, fields.length);
    assertEquals(0, expected.compareTo(fields[0].binaryValue()));
  }

  private static void assertMultiContains(
      Document d, String field, Object[] expected, Function<IndexableField, Object> value) {
    assertNotNull(d.get(field));
    IndexableField[] fields = d.getFields(field);
    assertEquals(expected.length, fields.length);
    for (IndexableField f : fields) {
      Object actual = value.apply(f);
      assertTrue(arrayContains(expected, actual));
    }
  }

  private static void assertBinaryMultiContains(Document d, String field, BytesRef[] expected) {
    IndexableField[] fields = d.getFields(field);
    assertEquals(expected.length, fields.length);
    for (IndexableField f : fields) {
      BytesRef actual = f.binaryValue();
      assertTrue(arrayBinaryContains(expected, actual));
    }
  }

  private static boolean arrayContains(Object[] array, Object value) {
    for (Object o : array) {
      if (Objects.equals(o, value)) {
        return true;
      }
    }
    return false;
  }

  private static boolean arrayBinaryContains(BytesRef[] array, BytesRef value) {
    for (BytesRef b : array) {
      if (b.compareTo(value) == 0) {
        return true;
      }
    }
    return false;
  }

  public void testIntegerNumericDocValue() throws IOException {
    // MemoryIndex used to fail when doc values are enabled and numericValue() returns an Integer
    // such as with IntField.
    FieldType ft = new FieldType();
    ft.setDocValuesType(DocValuesType.NUMERIC);
    ft.freeze();
    Field field =
        new Field("field", ft) {
          {
            fieldsData = 35;
          }
        };

    FieldType multiFt = new FieldType();
    multiFt.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    multiFt.freeze();
    Field multiField =
        new Field("multi_field", multiFt) {
          {
            fieldsData = 42;
          }
        };

    Field intField = new IntField("int_field", 50, Store.NO);

    MemoryIndex index = MemoryIndex.fromDocument(Arrays.asList(field, multiField, intField), null);
    IndexSearcher searcher = index.createSearcher();

    NumericDocValues ndv =
        searcher.getIndexReader().leaves().get(0).reader().getNumericDocValues("field");
    assertTrue(ndv.advanceExact(0));
    assertEquals(35, ndv.longValue());

    SortedNumericDocValues sndv =
        searcher.getIndexReader().leaves().get(0).reader().getSortedNumericDocValues("multi_field");
    assertTrue(sndv.advanceExact(0));
    assertEquals(1, sndv.docValueCount());
    assertEquals(42, sndv.nextValue());

    sndv =
        searcher.getIndexReader().leaves().get(0).reader().getSortedNumericDocValues("int_field");
    assertTrue(sndv.advanceExact(0));
    assertEquals(1, sndv.docValueCount());
    assertEquals(50, sndv.nextValue());
  }

  private static class MockIndexableField implements IndexableField {

    private final String field;
    private final BytesRef value;
    private final IndexableFieldType fieldType;

    MockIndexableField(String field, BytesRef value, IndexableFieldType fieldType) {
      this.field = field;
      this.value = value;
      this.fieldType = fieldType;
    }

    @Override
    public String name() {
      return field;
    }

    @Override
    public IndexableFieldType fieldType() {
      return fieldType;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
      return null;
    }

    @Override
    public BytesRef binaryValue() {
      return value;
    }

    @Override
    public String stringValue() {
      return null;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public Number numericValue() {
      return null;
    }

    @Override
    public StoredValue storedValue() {
      return null;
    }

    @Override
    public InvertableType invertableType() {
      return InvertableType.BINARY;
    }
  }

  public void testKeywordWithoutTokenStream() throws IOException {
    List<FieldType> legalFieldTypes = new ArrayList<>();
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS);
      ft.setOmitNorms(false);
      ft.freeze();
      legalFieldTypes.add(ft);
    }
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
      ft.setOmitNorms(false);
      ft.freeze();
      legalFieldTypes.add(ft);
    }
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS);
      ft.setOmitNorms(true);
      ft.freeze();
      legalFieldTypes.add(ft);
    }
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
      ft.setOmitNorms(true);
      ft.freeze();
      legalFieldTypes.add(ft);
    }
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS);
      ft.setStoreTermVectors(true);
      ft.freeze();
      legalFieldTypes.add(ft);
    }
    {
      FieldType ft = new FieldType();
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
      ft.setStoreTermVectors(true);
      ft.freeze();
      legalFieldTypes.add(ft);
    }

    for (FieldType ft : legalFieldTypes) {
      MockIndexableField field = new MockIndexableField("field", new BytesRef("a"), ft);
      MemoryIndex index = MemoryIndex.fromDocument(Arrays.asList(field, field), null);
      LeafReader leafReader = index.createSearcher().getIndexReader().leaves().get(0).reader();
      {
        Terms terms = leafReader.terms("field");
        assertEquals(1, terms.getSumDocFreq());
        assertEquals(2, terms.getSumTotalTermFreq());
        TermsEnum termsEnum = terms.iterator();
        assertTrue(termsEnum.seekExact(new BytesRef("a")));
        PostingsEnum pe = termsEnum.postings(null, PostingsEnum.ALL);
        assertEquals(0, pe.nextDoc());
        assertEquals(2, pe.freq());
        assertEquals(0, pe.nextPosition());
        assertEquals(1, pe.nextPosition());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
      }

      if (ft.storeTermVectors()) {
        Terms tvTerms = leafReader.termVectors().get(0).terms("field");
        assertEquals(1, tvTerms.getSumDocFreq());
        assertEquals(2, tvTerms.getSumTotalTermFreq());
        TermsEnum tvTermsEnum = tvTerms.iterator();
        assertTrue(tvTermsEnum.seekExact(new BytesRef("a")));
        PostingsEnum pe = tvTermsEnum.postings(null, PostingsEnum.ALL);
        assertEquals(0, pe.nextDoc());
        assertEquals(2, pe.freq());
        assertEquals(0, pe.nextPosition());
        assertEquals(1, pe.nextPosition());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, pe.nextDoc());
      }
    }
  }
}
