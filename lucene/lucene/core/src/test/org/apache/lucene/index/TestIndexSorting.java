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

package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.core.StringContains.containsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;

public class TestIndexSorting extends LuceneTestCase {
  static class AssertingNeedsIndexSortCodec extends FilterCodec {
    boolean needsIndexSort;
    int numCalls;

    AssertingNeedsIndexSortCodec() {
      super(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec());
    }

    @Override
    public PointsFormat pointsFormat() {
      final PointsFormat pf = delegate.pointsFormat();
      return new PointsFormat() {
        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
          final PointsWriter writer = pf.fieldsWriter(state);
          return new PointsWriter() {
            @Override
            public void merge(MergeState mergeState) throws IOException {
              // For single segment merge we cannot infer if the segment is already sorted or not.
              if (mergeState.docMaps.length > 1) {
                assertEquals(needsIndexSort, mergeState.needsIndexSort);
              }
              ++numCalls;
              writer.merge(mergeState);
            }

            @Override
            public void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException {
              writer.writeField(fieldInfo, values);
            }

            @Override
            public void finish() throws IOException {
              writer.finish();
            }

            @Override
            public void close() throws IOException {
              writer.close();
            }
          };
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) throws IOException {
          return pf.fieldsReader(state);
        }
      };
    }
  }

  private static void assertNeedsIndexSortMerge(
      SortField sortField,
      Consumer<Document> defaultValueConsumer,
      Consumer<Document> randomValueConsumer)
      throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    AssertingNeedsIndexSortCodec codec = new AssertingNeedsIndexSortCodec();
    iwc.setCodec(codec);
    Sort indexSort = new Sort(sortField, new SortField("id", SortField.Type.INT));
    iwc.setIndexSort(indexSort);
    LogMergePolicy policy = newLogMergePolicy();
    // make sure that merge factor is always > 2 and target search concurrency is no more than 1 to
    // avoid creating merges that are accidentally sorted
    policy.setTargetSearchConcurrency(1);
    if (policy.getMergeFactor() <= 2) {
      policy.setMergeFactor(3);
    }
    iwc.setMergePolicy(policy);

    // add already sorted documents
    codec.numCalls = 0;
    codec.needsIndexSort = false;
    IndexWriter w = new IndexWriter(dir, iwc);
    boolean withValues = random().nextBoolean();
    for (int i = 100; i < 200; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(new IntPoint("point", random().nextInt()));
      if (withValues) {
        defaultValueConsumer.accept(doc);
      }
      w.addDocument(doc);
      if (i % 10 == 0) {
        w.commit();
      }
    }
    Set<Integer> deletedDocs = new HashSet<>();
    int num = random().nextInt(20);
    for (int i = 0; i < num; i++) {
      int nextDoc = random().nextInt(100);
      w.deleteDocuments(new Term("id", Integer.toString(nextDoc)));
      deletedDocs.add(nextDoc);
    }
    w.commit();
    w.waitForMerges();
    w.forceMerge(1);
    assertTrue(codec.numCalls > 0);

    // merge sort is needed
    w.deleteAll();
    codec.numCalls = 0;
    codec.needsIndexSort = true;
    for (int i = 10; i >= 0; i--) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(new IntPoint("point", random().nextInt()));
      if (withValues) {
        defaultValueConsumer.accept(doc);
      }
      w.addDocument(doc);
      w.commit();
    }
    w.commit();
    w.waitForMerges();
    w.forceMerge(1);
    assertTrue(codec.numCalls > 0);

    // segment sort is needed
    codec.needsIndexSort = true;
    codec.numCalls = 0;
    for (int i = 201; i < 300; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(new IntPoint("point", random().nextInt()));
      randomValueConsumer.accept(doc);
      w.addDocument(doc);
      if (i % 10 == 0) {
        w.commit();
      }
    }
    w.commit();
    w.waitForMerges();
    w.forceMerge(1);
    assertTrue(codec.numCalls > 0);

    w.close();
    dir.close();
  }

  public void testNumericAlreadySorted() throws Exception {
    assertNeedsIndexSortMerge(
        new SortField("foo", SortField.Type.INT),
        (doc) -> doc.add(new NumericDocValuesField("foo", 0)),
        (doc) -> doc.add(new NumericDocValuesField("foo", random().nextInt())));
  }

  public void testStringAlreadySorted() throws Exception {
    assertNeedsIndexSortMerge(
        new SortField("foo", SortField.Type.STRING),
        (doc) -> doc.add(new SortedDocValuesField("foo", newBytesRef("default"))),
        (doc) -> doc.add(new SortedDocValuesField("foo", TestUtil.randomBinaryTerm(random()))));
  }

  public void testMultiValuedNumericAlreadySorted() throws Exception {
    assertNeedsIndexSortMerge(
        new SortedNumericSortField("foo", SortField.Type.INT),
        (doc) -> {
          doc.add(new SortedNumericDocValuesField("foo", Integer.MIN_VALUE));
          int num = random().nextInt(5);
          for (int j = 0; j < num; j++) {
            doc.add(new SortedNumericDocValuesField("foo", random().nextInt()));
          }
        },
        (doc) -> {
          int num = random().nextInt(5);
          for (int j = 0; j < num; j++) {
            doc.add(new SortedNumericDocValuesField("foo", random().nextInt()));
          }
        });
  }

  public void testMultiValuedStringAlreadySorted() throws Exception {
    assertNeedsIndexSortMerge(
        new SortedSetSortField("foo", false),
        (doc) -> {
          doc.add(new SortedSetDocValuesField("foo", newBytesRef("")));
          int num = random().nextInt(5);
          for (int j = 0; j < num; j++) {
            doc.add(new SortedSetDocValuesField("foo", TestUtil.randomBinaryTerm(random())));
          }
        },
        (doc) -> {
          int num = random().nextInt(5);
          for (int j = 0; j < num; j++) {
            doc.add(new SortedSetDocValuesField("foo", TestUtil.randomBinaryTerm(random())));
          }
        });
  }

  public void testBasicString() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.STRING));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("foo", newBytesRef("zzz")));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", newBytesRef("aaa")));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new SortedDocValuesField("foo", newBytesRef("mmm")));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    SortedDocValues values = leaf.getSortedDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals("aaa", values.lookupOrd(values.ordValue()).utf8ToString());
    assertEquals(1, values.nextDoc());
    assertEquals("mmm", values.lookupOrd(values.ordValue()).utf8ToString());
    assertEquals(2, values.nextDoc());
    assertEquals("zzz", values.lookupOrd(values.ordValue()).utf8ToString());
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicMultiValuedString() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedSetSortField("foo", false));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", 3));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzz")));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 1));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("aaa")));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzz")));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("bcg")));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 2));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("mmm")));
    doc.add(new SortedSetDocValuesField("foo", newBytesRef("pppp")));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("id");
    assertEquals(0, values.nextDoc());
    assertEquals(1l, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(2l, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(3l, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testMissingStringFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.STRING, reverse);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new SortedDocValuesField("foo", newBytesRef("zzz")));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new SortedDocValuesField("foo", newBytesRef("mmm")));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      SortedDocValues values = leaf.getSortedDocValues("foo");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals("zzz", values.lookupOrd(values.ordValue()).utf8ToString());
        assertEquals(1, values.nextDoc());
        assertEquals("mmm", values.lookupOrd(values.ordValue()).utf8ToString());
      } else {
        // docID 0 is missing:
        assertEquals(1, values.nextDoc());
        assertEquals("mmm", values.lookupOrd(values.ordValue()).utf8ToString());
        assertEquals(2, values.nextDoc());
        assertEquals("zzz", values.lookupOrd(values.ordValue()).utf8ToString());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedStringFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedSetSortField("foo", reverse);
      sortField.setMissingValue(SortField.STRING_FIRST);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzz")));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzza")));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzzd")));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("mmm")));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("nnnn")));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3l, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2l, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1l, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1l, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2l, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3l, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingStringLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.STRING, reverse);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new SortedDocValuesField("foo", newBytesRef("zzz")));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new SortedDocValuesField("foo", newBytesRef("mmm")));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      SortedDocValues values = leaf.getSortedDocValues("foo");
      if (reverse) {
        assertEquals(1, values.nextDoc());
        assertEquals("zzz", values.lookupOrd(values.ordValue()).utf8ToString());
        assertEquals(2, values.nextDoc());
        assertEquals("mmm", values.lookupOrd(values.ordValue()).utf8ToString());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals("mmm", values.lookupOrd(values.ordValue()).utf8ToString());
        assertEquals(1, values.nextDoc());
        assertEquals("zzz", values.lookupOrd(values.ordValue()).utf8ToString());
      }
      assertEquals(NO_MORE_DOCS, values.nextDoc());
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedStringLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedSetSortField("foo", reverse);
      sortField.setMissingValue(SortField.STRING_LAST);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzz")));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("zzzd")));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("mmm")));
      doc.add(new SortedSetDocValuesField("foo", newBytesRef("ppp")));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3l, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2l, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1l, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1l, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2l, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3l, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testBasicLong() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 18));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", -1));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", 7));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals(-1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(7, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(18, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicMultiValuedLong() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", 3));
    doc.add(new SortedNumericDocValuesField("foo", 18));
    doc.add(new SortedNumericDocValuesField("foo", 35));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 1));
    doc.add(new SortedNumericDocValuesField("foo", -1));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 2));
    doc.add(new SortedNumericDocValuesField("foo", 7));
    doc.add(new SortedNumericDocValuesField("foo", 22));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("id");
    assertEquals(0, values.nextDoc());
    assertEquals(1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(2, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(3, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testMissingLongFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.LONG, reverse);
      sortField.setMissingValue(Long.valueOf(Long.MIN_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", 18));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("foo", 7));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(18, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(7, values.longValue());
      } else {
        // docID 0 has no value
        assertEquals(1, values.nextDoc());
        assertEquals(7, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(18, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedLongFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.LONG, reverse);
      sortField.setMissingValue(Long.valueOf(Long.MIN_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      doc.add(new SortedNumericDocValuesField("foo", 18));
      doc.add(new SortedNumericDocValuesField("foo", 27));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", 7));
      doc.add(new SortedNumericDocValuesField("foo", 24));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingLongLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.LONG, reverse);
      sortField.setMissingValue(Long.valueOf(Long.MAX_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", 18));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("foo", 7));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        // docID 0 is missing
        assertEquals(1, values.nextDoc());
        assertEquals(18, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(7, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(7, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(18, values.longValue());
      }
      assertEquals(NO_MORE_DOCS, values.nextDoc());
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedLongLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.LONG, reverse);
      sortField.setMissingValue(Long.valueOf(Long.MAX_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", 18));
      doc.add(new SortedNumericDocValuesField("foo", 65));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      doc.add(new SortedNumericDocValuesField("foo", 7));
      doc.add(new SortedNumericDocValuesField("foo", 34));
      doc.add(new SortedNumericDocValuesField("foo", 74));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testBasicInt() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.INT));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 18));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", -1));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", 7));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals(-1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(7, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(18, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicMultiValuedInt() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("foo", SortField.Type.INT));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", 3));
    doc.add(new SortedNumericDocValuesField("foo", 18));
    doc.add(new SortedNumericDocValuesField("foo", 34));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 1));
    doc.add(new SortedNumericDocValuesField("foo", -1));
    doc.add(new SortedNumericDocValuesField("foo", 34));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 2));
    doc.add(new SortedNumericDocValuesField("foo", 7));
    doc.add(new SortedNumericDocValuesField("foo", 22));
    doc.add(new SortedNumericDocValuesField("foo", 27));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("id");
    assertEquals(0, values.nextDoc());
    assertEquals(1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(2, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(3, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testMissingIntFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.INT, reverse);
      sortField.setMissingValue(Integer.valueOf(Integer.MIN_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", 18));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("foo", 7));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(18, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(7, values.longValue());
      } else {
        assertEquals(1, values.nextDoc());
        assertEquals(7, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(18, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedIntFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.INT, reverse);
      sortField.setMissingValue(Integer.valueOf(Integer.MIN_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      doc.add(new SortedNumericDocValuesField("foo", 18));
      doc.add(new SortedNumericDocValuesField("foo", 187667));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", 7));
      doc.add(new SortedNumericDocValuesField("foo", 34));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingIntLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.INT, reverse);
      sortField.setMissingValue(Integer.valueOf(Integer.MAX_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", 18));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("foo", 7));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        // docID 0 is missing
        assertEquals(1, values.nextDoc());
        assertEquals(18, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(7, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(7, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(18, values.longValue());
      }
      assertEquals(NO_MORE_DOCS, values.nextDoc());
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedIntLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.INT, reverse);
      sortField.setMissingValue(Integer.valueOf(Integer.MAX_VALUE));
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", 18));
      doc.add(new SortedNumericDocValuesField("foo", 6372));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      doc.add(new SortedNumericDocValuesField("foo", 7));
      doc.add(new SortedNumericDocValuesField("foo", 8));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testBasicDouble() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.DOUBLE));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DoubleDocValuesField("foo", 18.0));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new DoubleDocValuesField("foo", -1.0));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new DoubleDocValuesField("foo", 7.0));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals(-1.0, Double.longBitsToDouble(values.longValue()), 0.0);
    assertEquals(1, values.nextDoc());
    assertEquals(7.0, Double.longBitsToDouble(values.longValue()), 0.0);
    assertEquals(2, values.nextDoc());
    assertEquals(18.0, Double.longBitsToDouble(values.longValue()), 0.0);
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicMultiValuedDouble() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("foo", SortField.Type.DOUBLE));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", 3));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.54)));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(27.0)));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 1));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(-1.0)));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(0.0)));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 2));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.0)));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.67)));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("id");
    assertEquals(0, values.nextDoc());
    assertEquals(1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(2, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(3, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testMissingDoubleFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.DOUBLE, reverse);
      sortField.setMissingValue(Double.NEGATIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new DoubleDocValuesField("foo", 18.0));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new DoubleDocValuesField("foo", 7.0));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(18.0, Double.longBitsToDouble(values.longValue()), 0.0);
        assertEquals(1, values.nextDoc());
        assertEquals(7.0, Double.longBitsToDouble(values.longValue()), 0.0);
      } else {
        assertEquals(1, values.nextDoc());
        assertEquals(7.0, Double.longBitsToDouble(values.longValue()), 0.0);
        assertEquals(2, values.nextDoc());
        assertEquals(18.0, Double.longBitsToDouble(values.longValue()), 0.0);
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedDoubleFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.DOUBLE, reverse);
      sortField.setMissingValue(Double.NEGATIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(18.0)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(18.76)));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.0)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(70.0)));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingDoubleLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.DOUBLE, reverse);
      sortField.setMissingValue(Double.POSITIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new DoubleDocValuesField("foo", 18.0));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new DoubleDocValuesField("foo", 7.0));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(1, values.nextDoc());
        assertEquals(18.0, Double.longBitsToDouble(values.longValue()), 0.0);
        assertEquals(2, values.nextDoc());
        assertEquals(7.0, Double.longBitsToDouble(values.longValue()), 0.0);
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(7.0, Double.longBitsToDouble(values.longValue()), 0.0);
        assertEquals(1, values.nextDoc());
        assertEquals(18.0, Double.longBitsToDouble(values.longValue()), 0.0);
      }
      assertEquals(NO_MORE_DOCS, values.nextDoc());
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedDoubleLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.DOUBLE, reverse);
      sortField.setMissingValue(Double.POSITIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(18.0)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(8262.0)));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.0)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(7.87)));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testBasicFloat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.FLOAT));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new FloatDocValuesField("foo", 18.0f));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new FloatDocValuesField("foo", -1.0f));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new FloatDocValuesField("foo", 7.0f));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals(-1.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
    assertEquals(1, values.nextDoc());
    assertEquals(7.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
    assertEquals(2, values.nextDoc());
    assertEquals(18.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicMultiValuedFloat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("foo", SortField.Type.FLOAT));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("id", 3));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(18.0f)));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(29.0f)));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a
    // sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 1));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(-1.0f)));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(34.0f)));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("id", 2));
    doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(7.0f)));
    w.addDocument(doc);
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("id");
    assertEquals(0, values.nextDoc());
    assertEquals(1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(2, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(3, values.longValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testMissingFloatFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.FLOAT, reverse);
      sortField.setMissingValue(Float.NEGATIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new FloatDocValuesField("foo", 18.0f));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new FloatDocValuesField("foo", 7.0f));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(18.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
        assertEquals(1, values.nextDoc());
        assertEquals(7.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
      } else {
        assertEquals(1, values.nextDoc());
        assertEquals(7.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
        assertEquals(2, values.nextDoc());
        assertEquals(18.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedFloatFirst() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.FLOAT, reverse);
      sortField.setMissingValue(Float.NEGATIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(18.0f)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(726.0f)));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(7.0f)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(18.0f)));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingFloatLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortField("foo", SortField.Type.FLOAT, reverse);
      sortField.setMissingValue(Float.POSITIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new FloatDocValuesField("foo", 18.0f));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      w.addDocument(new Document());
      w.commit();

      doc = new Document();
      doc.add(new FloatDocValuesField("foo", 7.0f));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("foo");
      if (reverse) {
        assertEquals(1, values.nextDoc());
        assertEquals(18.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
        assertEquals(2, values.nextDoc());
        assertEquals(7.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(7.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
        assertEquals(1, values.nextDoc());
        assertEquals(18.0f, Float.intBitsToFloat((int) values.longValue()), 0.0f);
      }
      assertEquals(NO_MORE_DOCS, values.nextDoc());
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testMissingMultiValuedFloatLast() throws Exception {
    for (boolean reverse : new boolean[] {true, false}) {
      Directory dir = newDirectory();
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      SortField sortField = new SortedNumericSortField("foo", SortField.Type.FLOAT, reverse);
      sortField.setMissingValue(Float.POSITIVE_INFINITY);
      Sort indexSort = new Sort(sortField);
      iwc.setIndexSort(indexSort);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", 2));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(726.0f)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(18.0f)));
      w.addDocument(doc);
      // so we get more than one segment, so that forceMerge actually does merge, since we only get
      // a sorted segment by merging:
      w.commit();

      // missing
      doc = new Document();
      doc.add(new NumericDocValuesField("id", 3));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new NumericDocValuesField("id", 1));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(12.67f)));
      doc.add(new SortedNumericDocValuesField("foo", NumericUtils.floatToSortableInt(7.0f)));
      w.addDocument(doc);
      w.forceMerge(1);

      DirectoryReader r = DirectoryReader.open(w);
      LeafReader leaf = getOnlyLeafReader(r);
      assertEquals(3, leaf.maxDoc());
      NumericDocValues values = leaf.getNumericDocValues("id");
      if (reverse) {
        assertEquals(0, values.nextDoc());
        assertEquals(3, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(1, values.longValue());
      } else {
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.longValue());
        assertEquals(1, values.nextDoc());
        assertEquals(2, values.longValue());
        assertEquals(2, values.nextDoc());
        assertEquals(3, values.longValue());
      }
      r.close();
      w.close();
      dir.close();
    }
  }

  public void testRandom1() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    final int numDocs = atLeast(200);
    final FixedBitSet deleted = new FixedBitSet(numDocs);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("foo", random().nextInt(20)));
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      w.addDocument(doc);
      if (random().nextInt(5) == 0) {
        DirectoryReader.open(w).close();
      } else if (random().nextInt(30) == 0) {
        w.forceMerge(2);
      } else if (random().nextInt(4) == 0) {
        final int id = TestUtil.nextInt(random(), 0, i);
        deleted.set(id);
        w.deleteDocuments(new Term("id", Integer.toString(id)));
      }
    }

    // Check that segments are sorted
    DirectoryReader reader = DirectoryReader.open(w);
    for (LeafReaderContext ctx : reader.leaves()) {
      final SegmentReader leaf = (SegmentReader) ctx.reader();
      SegmentInfo info = leaf.getSegmentInfo().info;
      switch (info.getDiagnostics().get(IndexWriter.SOURCE)) {
        case IndexWriter.SOURCE_FLUSH:
        case IndexWriter.SOURCE_MERGE:
          assertEquals(indexSort, info.getIndexSort());
          final NumericDocValues values = leaf.getNumericDocValues("foo");
          long previous = Long.MIN_VALUE;
          for (int i = 0; i < leaf.maxDoc(); ++i) {
            assertEquals(i, values.nextDoc());
            final long value = values.longValue();
            assertTrue(value >= previous);
            previous = value;
          }
          break;
        default:
          fail();
      }
    }

    // Now check that the index is consistent
    IndexSearcher searcher = newSearcher(reader);
    StoredFields storedFields = reader.storedFields();
    for (int i = 0; i < numDocs; ++i) {
      TermQuery termQuery = new TermQuery(new Term("id", Integer.toString(i)));
      final TopDocs topDocs = searcher.search(termQuery, 1);
      if (deleted.get(i)) {
        assertEquals(0, topDocs.totalHits.value);
      } else {
        assertEquals(1, topDocs.totalHits.value);
        NumericDocValues values = MultiDocValues.getNumericValues(reader, "id");
        assertEquals(topDocs.scoreDocs[0].doc, values.advance(topDocs.scoreDocs[0].doc));
        assertEquals(i, values.longValue());
        Document document = storedFields.document(topDocs.scoreDocs[0].doc);
        assertEquals(Integer.toString(i), document.get("id"));
      }
    }

    reader.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedRandom1() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    final int numDocs = atLeast(200);
    final FixedBitSet deleted = new FixedBitSet(numDocs);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int num = random().nextInt(10);
      for (int j = 0; j < num; j++) {
        doc.add(new SortedNumericDocValuesField("foo", random().nextInt(2000)));
      }
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      w.addDocument(doc);
      if (random().nextInt(5) == 0) {
        DirectoryReader.open(w).close();
      } else if (random().nextInt(30) == 0) {
        w.forceMerge(2);
      } else if (random().nextInt(4) == 0) {
        final int id = TestUtil.nextInt(random(), 0, i);
        deleted.set(id);
        w.deleteDocuments(new Term("id", Integer.toString(id)));
      }
    }

    DirectoryReader reader = DirectoryReader.open(w);
    // Now check that the index is consistent
    IndexSearcher searcher = newSearcher(reader);
    StoredFields storedFields = reader.storedFields();
    for (int i = 0; i < numDocs; ++i) {
      TermQuery termQuery = new TermQuery(new Term("id", Integer.toString(i)));
      final TopDocs topDocs = searcher.search(termQuery, 1);
      if (deleted.get(i)) {
        assertEquals(0, topDocs.totalHits.value);
      } else {
        assertEquals(1, topDocs.totalHits.value);
        NumericDocValues values = MultiDocValues.getNumericValues(reader, "id");
        assertEquals(topDocs.scoreDocs[0].doc, values.advance(topDocs.scoreDocs[0].doc));
        assertEquals(i, values.longValue());
        Document document = storedFields.document(topDocs.scoreDocs[0].doc);
        assertEquals(Integer.toString(i), document.get("id"));
      }
    }

    reader.close();
    w.close();
    dir.close();
  }

  static class UpdateRunnable implements Runnable {

    private final int numDocs;
    private final Random random;
    private final AtomicInteger updateCount;
    private final IndexWriter w;
    private final Map<Integer, Long> values;
    private final CountDownLatch latch;

    UpdateRunnable(
        int numDocs,
        Random random,
        CountDownLatch latch,
        AtomicInteger updateCount,
        IndexWriter w,
        Map<Integer, Long> values) {
      this.numDocs = numDocs;
      this.random = random;
      this.latch = latch;
      this.updateCount = updateCount;
      this.w = w;
      this.values = values;
    }

    @Override
    public void run() {
      try {
        latch.await();
        while (updateCount.decrementAndGet() >= 0) {
          final int id = random.nextInt(numDocs);
          final long value = random.nextInt(20);
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(id), Store.NO));
          doc.add(new NumericDocValuesField("foo", value));

          synchronized (values) {
            w.updateDocument(new Term("id", Integer.toString(id)), doc);
            values.put(id, value);
          }

          switch (random.nextInt(10)) {
            case 0:
            case 1:
              // reopen
              DirectoryReader.open(w).close();
              break;
            case 2:
              w.forceMerge(3);
              break;
          }
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // There is tricky logic to resolve deletes that happened while merging
  public void testConcurrentUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Map<Integer, Long> values = new HashMap<>();

    final int numDocs = atLeast(100);
    Thread[] threads = new Thread[2];

    final AtomicInteger updateCount = new AtomicInteger(atLeast(1000));
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < threads.length; ++i) {
      Random r = new Random(random().nextLong());
      threads[i] = new Thread(new UpdateRunnable(numDocs, r, latch, updateCount, w, values));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    latch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < numDocs; ++i) {
      final TopDocs topDocs =
          searcher.search(new TermQuery(new Term("id", Integer.toString(i))), 1);
      if (values.containsKey(i) == false) {
        assertEquals(0, topDocs.totalHits.value);
      } else {
        assertEquals(1, topDocs.totalHits.value);
        NumericDocValues dvs = MultiDocValues.getNumericValues(reader, "foo");
        int docID = topDocs.scoreDocs[0].doc;
        assertEquals(docID, dvs.advance(docID));
        assertEquals(values.get(i).longValue(), dvs.longValue());
      }
    }
    reader.close();
    w.close();
    dir.close();
  }

  // docvalues fields involved in the index sort cannot be updated
  public void testBadDVUpdate() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("id", newBytesRef("0"), Store.NO));
    doc.add(new NumericDocValuesField("foo", random().nextInt()));
    w.addDocument(doc);
    w.commit();
    IllegalArgumentException exc =
        expectThrows(
            IllegalArgumentException.class,
            () -> w.updateDocValues(new Term("id", "0"), new NumericDocValuesField("foo", -1)));
    assertEquals(
        exc.getMessage(),
        "cannot update docvalues field involved in the index sort, field=foo, sort=<long: \"foo\">");
    exc =
        expectThrows(
            IllegalArgumentException.class,
            () -> w.updateNumericDocValue(new Term("id", "0"), "foo", -1));
    assertEquals(
        exc.getMessage(),
        "cannot update docvalues field involved in the index sort, field=foo, sort=<long: \"foo\">");
    w.close();
    dir.close();
  }

  static class DVUpdateRunnable implements Runnable {

    private final int numDocs;
    private final Random random;
    private final AtomicInteger updateCount;
    private final IndexWriter w;
    private final Map<Integer, Long> values;
    private final CountDownLatch latch;

    DVUpdateRunnable(
        int numDocs,
        Random random,
        CountDownLatch latch,
        AtomicInteger updateCount,
        IndexWriter w,
        Map<Integer, Long> values) {
      this.numDocs = numDocs;
      this.random = random;
      this.latch = latch;
      this.updateCount = updateCount;
      this.w = w;
      this.values = values;
    }

    @Override
    public void run() {
      try {
        latch.await();
        while (updateCount.decrementAndGet() >= 0) {
          final int id = random.nextInt(numDocs);
          final long value = random.nextInt(20);

          synchronized (values) {
            w.updateDocValues(
                new Term("id", Integer.toString(id)), new NumericDocValuesField("bar", value));
            values.put(id, value);
          }

          switch (random.nextInt(10)) {
            case 0:
            case 1:
              // reopen
              DirectoryReader.open(w).close();
              break;
            case 2:
              w.forceMerge(3);
              break;
          }
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // There is tricky logic to resolve dv updates that happened while merging
  public void testConcurrentDVUpdates() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Map<Integer, Long> values = new HashMap<>();

    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.NO));
      doc.add(new NumericDocValuesField("foo", random().nextInt()));
      doc.add(new NumericDocValuesField("bar", -1));
      w.addDocument(doc);
      values.put(i, -1L);
    }
    Thread[] threads = new Thread[2];
    final AtomicInteger updateCount = new AtomicInteger(atLeast(1000));
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < threads.length; ++i) {
      Random r = new Random(random().nextLong());
      threads[i] = new Thread(new DVUpdateRunnable(numDocs, r, latch, updateCount, w, values));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    latch.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < numDocs; ++i) {
      final TopDocs topDocs =
          searcher.search(new TermQuery(new Term("id", Integer.toString(i))), 1);
      assertEquals(1, topDocs.totalHits.value);
      NumericDocValues dvs = MultiDocValues.getNumericValues(reader, "bar");
      int hitDoc = topDocs.scoreDocs[0].doc;
      assertEquals(hitDoc, dvs.advance(hitDoc));
      assertEquals(values.get(i).longValue(), dvs.longValue());
    }
    reader.close();
    w.close();
    dir.close();
  }

  public void testBadAddIndexes() throws Exception {
    Directory dir = newDirectory();
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.LONG));
    IndexWriterConfig iwc1 = newIndexWriterConfig();
    iwc1.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc1);
    w.addDocument(new Document());
    List<Sort> indexSorts =
        Arrays.asList(null, new Sort(new SortField("bar", SortField.Type.LONG)));
    for (Sort sort : indexSorts) {
      Directory dir2 = newDirectory();
      IndexWriterConfig iwc2 = newIndexWriterConfig();
      if (sort != null) {
        iwc2.setIndexSort(sort);
      }
      IndexWriter w2 = new IndexWriter(dir2, iwc2);
      w2.addDocument(new Document());
      final IndexReader reader = DirectoryReader.open(w2);
      w2.close();
      IllegalArgumentException expected =
          expectThrows(IllegalArgumentException.class, () -> w.addIndexes(dir2));
      assertThat(expected.getMessage(), containsString("cannot change index sort"));
      CodecReader[] codecReaders = new CodecReader[reader.leaves().size()];
      for (int i = 0; i < codecReaders.length; ++i) {
        codecReaders[i] = (CodecReader) reader.leaves().get(i).reader();
      }
      expected = expectThrows(IllegalArgumentException.class, () -> w.addIndexes(codecReaders));
      assertThat(expected.getMessage(), containsString("cannot change index sort"));

      reader.close();
      dir2.close();
    }
    w.close();
    dir.close();
  }

  public void testAddIndexes(boolean withDeletes, boolean useReaders) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc1 = newIndexWriterConfig();
    boolean useParent = rarely();
    if (useParent) {
      iwc1.setParentField("___parent");
    }
    Sort indexSort =
        new Sort(
            new SortField("foo", SortField.Type.LONG), new SortField("bar", SortField.Type.LONG));
    iwc1.setIndexSort(indexSort);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc1);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Store.NO));
      doc.add(new NumericDocValuesField("foo", random().nextInt(20)));
      doc.add(new NumericDocValuesField("bar", random().nextInt(20)));
      w.addDocument(doc);
    }
    if (withDeletes) {
      for (int i = random().nextInt(5); i < numDocs; i += TestUtil.nextInt(random(), 1, 5)) {
        w.deleteDocuments(new Term("id", Integer.toString(i)));
      }
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader reader = w.getReader();
    w.close();

    Directory dir2 = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    if (random().nextBoolean()) {
      // test congruent index sort
      iwc.setIndexSort(new Sort(new SortField("foo", SortField.Type.LONG)));
    } else {
      iwc.setIndexSort(indexSort);
    }
    if (useParent) {
      iwc.setParentField("___parent");
    }
    IndexWriter w2 = new IndexWriter(dir2, iwc);

    if (useReaders) {
      CodecReader[] codecReaders = new CodecReader[reader.leaves().size()];
      for (int i = 0; i < codecReaders.length; ++i) {
        codecReaders[i] = (CodecReader) reader.leaves().get(i).reader();
      }
      w2.addIndexes(codecReaders);
    } else {
      w2.addIndexes(dir);
    }
    final IndexReader reader2 = DirectoryReader.open(w2);
    final IndexSearcher searcher = newSearcher(reader);
    final IndexSearcher searcher2 = newSearcher(reader2);
    for (int i = 0; i < numDocs; ++i) {
      Query query = new TermQuery(new Term("id", Integer.toString(i)));
      final TopDocs topDocs = searcher.search(query, 1);
      final TopDocs topDocs2 = searcher2.search(query, 1);
      assertEquals(topDocs.totalHits.value, topDocs2.totalHits.value);
      if (topDocs.totalHits.value == 1) {
        NumericDocValues dvs1 = MultiDocValues.getNumericValues(reader, "foo");
        int hitDoc1 = topDocs.scoreDocs[0].doc;
        assertEquals(hitDoc1, dvs1.advance(hitDoc1));
        long value1 = dvs1.longValue();
        NumericDocValues dvs2 = MultiDocValues.getNumericValues(reader2, "foo");
        int hitDoc2 = topDocs2.scoreDocs[0].doc;
        assertEquals(hitDoc2, dvs2.advance(hitDoc2));
        long value2 = dvs2.longValue();
        assertEquals(value1, value2);
      }
    }

    IOUtils.close(reader, reader2, w2, dir, dir2);
  }

  public void testAddIndexes() throws Exception {
    testAddIndexes(false, true);
  }

  public void testAddIndexesWithDeletions() throws Exception {
    testAddIndexes(true, true);
  }

  public void testAddIndexesWithDirectory() throws Exception {
    testAddIndexes(false, false);
  }

  public void testAddIndexesWithDeletionsAndDirectory() throws Exception {
    testAddIndexes(true, false);
  }

  public void testBadSort() throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              iwc.setIndexSort(Sort.RELEVANCE);
            });
    assertEquals("Cannot sort index with sort field <score>", expected.getMessage());
  }

  // you can't change the index sort on an existing index:
  public void testIllegalChangeSort() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(new Sort(new SortField("foo", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    DirectoryReader.open(w).close();
    w.addDocument(new Document());
    w.forceMerge(1);
    w.close();

    final IndexWriterConfig iwc2 = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc2.setIndexSort(new Sort(new SortField("bar", SortField.Type.LONG)));
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new IndexWriter(dir, iwc2);
            });
    String message = e.getMessage();
    assertTrue(message.contains("cannot change previous indexSort=<long: \"foo\">"));
    assertTrue(message.contains("to new indexSort=<long: \"bar\">"));
    dir.close();
  }

  static final class NormsSimilarity extends Similarity {

    private final Similarity in;

    public NormsSimilarity(Similarity in) {
      this.in = in;
    }

    @Override
    public long computeNorm(FieldInvertState state) {
      if (state.getName().equals("norms")) {
        return state.getLength();
      } else {
        return in.computeNorm(state);
      }
    }

    @Override
    public SimScorer scorer(
        float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      return in.scorer(boost, collectionStats, termStats);
    }
  }

  static final class PositionsTokenStream extends TokenStream {

    private final CharTermAttribute term;
    private final PayloadAttribute payload;
    private final OffsetAttribute offset;

    private int pos, off;

    public PositionsTokenStream() {
      term = addAttribute(CharTermAttribute.class);
      payload = addAttribute(PayloadAttribute.class);
      offset = addAttribute(OffsetAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (pos == 0) {
        return false;
      }

      clearAttributes();
      term.append("#all#");
      payload.setPayload(newBytesRef(Integer.toString(pos)));
      offset.setOffset(off, off);
      --pos;
      ++off;
      return true;
    }

    void setId(int id) {
      pos = id / 10 + 1;
      off = 0;
    }
  }

  public void testRandom2() throws Exception {
    int numDocs = atLeast(100);

    FieldType POSITIONS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
    POSITIONS_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    POSITIONS_TYPE.freeze();

    FieldType TERM_VECTORS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
    TERM_VECTORS_TYPE.setStoreTermVectors(true);
    TERM_VECTORS_TYPE.freeze();

    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer();
            return new TokenStreamComponents(tokenizer, tokenizer);
          }
        };

    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      int id = i * 10;
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(id), Store.YES));
      doc.add(new StringField("docs", "#all#", Store.NO));
      PositionsTokenStream positions = new PositionsTokenStream();
      positions.setId(id);
      doc.add(new Field("positions", positions, POSITIONS_TYPE));
      doc.add(new NumericDocValuesField("numeric", id));
      String value =
          IntStream.range(0, id)
              .mapToObj(k -> Integer.toString(id))
              .collect(Collectors.joining(" "));
      TextField norms = new TextField("norms", value, Store.NO);
      doc.add(norms);
      doc.add(new BinaryDocValuesField("binary", newBytesRef(Integer.toString(id))));
      doc.add(new SortedDocValuesField("sorted", newBytesRef(Integer.toString(id))));
      doc.add(
          new SortedSetDocValuesField("multi_valued_string", newBytesRef(Integer.toString(id))));
      doc.add(
          new SortedSetDocValuesField(
              "multi_valued_string", newBytesRef(Integer.toString(id + 1))));
      doc.add(new SortedNumericDocValuesField("multi_valued_numeric", id));
      doc.add(new SortedNumericDocValuesField("multi_valued_numeric", id + 1));
      doc.add(new Field("term_vectors", Integer.toString(id), TERM_VECTORS_TYPE));
      byte[] bytes = new byte[4];
      NumericUtils.intToSortableBytes(id, bytes, 0);
      doc.add(new BinaryPoint("points", bytes));
      docs.add(doc);
    }

    // Must use the same seed for both RandomIndexWriters so they behave identically
    long seed = random().nextLong();

    // We add document alread in ID order for the first writer:
    Directory dir1 = newFSDirectory(createTempDir());

    Random random1 = new Random(seed);
    IndexWriterConfig iwc1 = newIndexWriterConfig(random1, a);
    iwc1.setSimilarity(new NormsSimilarity(iwc1.getSimilarity())); // for testing norms field
    // preserve docIDs
    iwc1.setMergePolicy(newLogMergePolicy());
    if (VERBOSE) {
      System.out.println("TEST: now index pre-sorted");
    }
    RandomIndexWriter w1 = new RandomIndexWriter(random1, dir1, iwc1);
    for (Document doc : docs) {
      ((PositionsTokenStream) ((Field) doc.getField("positions")).tokenStreamValue())
          .setId(Integer.parseInt(doc.get("id")));
      w1.addDocument(doc);
    }

    // We shuffle documents, but set index sort, for the second writer:
    Directory dir2 = newFSDirectory(createTempDir());

    Random random2 = new Random(seed);
    IndexWriterConfig iwc2 = newIndexWriterConfig(random2, a);
    iwc2.setSimilarity(new NormsSimilarity(iwc2.getSimilarity())); // for testing norms field

    Sort sort = new Sort(new SortField("numeric", SortField.Type.INT));
    iwc2.setIndexSort(sort);

    Collections.shuffle(docs, random());
    if (VERBOSE) {
      System.out.println("TEST: now index with index-time sorting");
    }
    RandomIndexWriter w2 = new RandomIndexWriter(random2, dir2, iwc2);
    int count = 0;
    int commitAtCount = TestUtil.nextInt(random(), 1, numDocs - 1);
    for (Document doc : docs) {
      ((PositionsTokenStream) ((Field) doc.getField("positions")).tokenStreamValue())
          .setId(Integer.parseInt(doc.get("id")));
      if (count++ == commitAtCount) {
        // Ensure forceMerge really does merge
        w2.commit();
      }
      w2.addDocument(doc);
    }
    if (VERBOSE) {
      System.out.println("TEST: now force merge");
    }
    w2.forceMerge(1);

    DirectoryReader r1 = w1.getReader();
    DirectoryReader r2 = w2.getReader();
    if (VERBOSE) {
      System.out.println("TEST: now compare r1=" + r1 + " r2=" + r2);
    }
    assertEquals(sort, getOnlyLeafReader(r2).getMetaData().getSort());
    assertReaderEquals("left: sorted by hand; right: sorted by Lucene", r1, r2);
    IOUtils.close(w1, w2, r1, r2, dir1, dir2);
  }

  private static final class RandomDoc {
    public final int intValue;
    public final int[] intValues;
    public final long longValue;
    public final long[] longValues;
    public final float floatValue;
    public final float[] floatValues;
    public final double doubleValue;
    public final double[] doubleValues;
    public final byte[] bytesValue;
    public final byte[][] bytesValues;

    public RandomDoc(int id) {
      intValue = random().nextInt();
      longValue = random().nextLong();
      floatValue = random().nextFloat();
      doubleValue = random().nextDouble();
      bytesValue = new byte[TestUtil.nextInt(random(), 1, 50)];
      random().nextBytes(bytesValue);

      int numValues = random().nextInt(10);
      intValues = new int[numValues];
      longValues = new long[numValues];
      floatValues = new float[numValues];
      doubleValues = new double[numValues];
      bytesValues = new byte[numValues][];
      for (int i = 0; i < numValues; i++) {
        intValues[i] = random().nextInt();
        longValues[i] = random().nextLong();
        floatValues[i] = random().nextFloat();
        doubleValues[i] = random().nextDouble();
        bytesValues[i] = new byte[TestUtil.nextInt(random(), 1, 50)];
        random().nextBytes(bytesValue);
      }
    }
  }

  private static SortField randomIndexSortField() {
    boolean reversed = random().nextBoolean();
    SortField sortField;
    switch (random().nextInt(10)) {
      case 0:
        sortField = new SortField("int", SortField.Type.INT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextInt());
        }
        break;
      case 1:
        sortField = new SortedNumericSortField("multi_valued_int", SortField.Type.INT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextInt());
        }
        break;
      case 2:
        sortField = new SortField("long", SortField.Type.LONG, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextLong());
        }
        break;
      case 3:
        sortField = new SortedNumericSortField("multi_valued_long", SortField.Type.LONG, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextLong());
        }
        break;
      case 4:
        sortField = new SortField("float", SortField.Type.FLOAT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextFloat());
        }
        break;
      case 5:
        sortField =
            new SortedNumericSortField("multi_valued_float", SortField.Type.FLOAT, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextFloat());
        }
        break;
      case 6:
        sortField = new SortField("double", SortField.Type.DOUBLE, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextDouble());
        }
        break;
      case 7:
        sortField =
            new SortedNumericSortField("multi_valued_double", SortField.Type.DOUBLE, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(random().nextDouble());
        }
        break;
      case 8:
        sortField = new SortField("bytes", SortField.Type.STRING, reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(SortField.STRING_LAST);
        }
        break;
      case 9:
        sortField = new SortedSetSortField("multi_valued_bytes", reversed);
        if (random().nextBoolean()) {
          sortField.setMissingValue(SortField.STRING_LAST);
        }
        break;
      default:
        sortField = null;
        fail();
    }
    return sortField;
  }

  private static Sort randomSort() {
    // at least 2
    int numFields = TestUtil.nextInt(random(), 2, 4);
    SortField[] sortFields = new SortField[numFields];
    for (int i = 0; i < numFields - 1; i++) {
      SortField sortField = randomIndexSortField();
      sortFields[i] = sortField;
    }

    // tie-break by id:
    sortFields[numFields - 1] = new SortField("id", SortField.Type.INT);

    return new Sort(sortFields);
  }

  // pits index time sorting against query time sorting
  public void testRandom3() throws Exception {
    int numDocs = atLeast(1000);
    List<RandomDoc> docs = new ArrayList<>();

    Sort sort = randomSort();
    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs + " use sort=" + sort);
    }

    // no index sorting, all search-time sorting:
    Directory dir1 = newFSDirectory(createTempDir());
    IndexWriterConfig iwc1 = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w1 = new IndexWriter(dir1, iwc1);

    // use index sorting:
    Directory dir2 = newFSDirectory(createTempDir());
    IndexWriterConfig iwc2 = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc2.setIndexSort(sort);
    IndexWriter w2 = new IndexWriter(dir2, iwc2);

    Set<Integer> toDelete = new HashSet<>();

    double deleteChance = random().nextDouble();

    for (int id = 0; id < numDocs; id++) {
      RandomDoc docValues = new RandomDoc(id);
      docs.add(docValues);
      if (VERBOSE) {
        System.out.println("TEST: doc id=" + id);
        System.out.println("  int=" + docValues.intValue);
        System.out.println("  long=" + docValues.longValue);
        System.out.println("  float=" + docValues.floatValue);
        System.out.println("  double=" + docValues.doubleValue);
        System.out.println("  bytes=" + newBytesRef(docValues.bytesValue));
        System.out.println("  mvf=" + Arrays.toString(docValues.floatValues));
      }

      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
      doc.add(new NumericDocValuesField("id", id));
      doc.add(new NumericDocValuesField("int", docValues.intValue));
      doc.add(new NumericDocValuesField("long", docValues.longValue));
      doc.add(new DoubleDocValuesField("double", docValues.doubleValue));
      doc.add(new FloatDocValuesField("float", docValues.floatValue));
      doc.add(new SortedDocValuesField("bytes", newBytesRef(docValues.bytesValue)));

      for (int value : docValues.intValues) {
        doc.add(new SortedNumericDocValuesField("multi_valued_int", value));
      }

      for (long value : docValues.longValues) {
        doc.add(new SortedNumericDocValuesField("multi_valued_long", value));
      }

      for (float value : docValues.floatValues) {
        doc.add(
            new SortedNumericDocValuesField(
                "multi_valued_float", NumericUtils.floatToSortableInt(value)));
      }

      for (double value : docValues.doubleValues) {
        doc.add(
            new SortedNumericDocValuesField(
                "multi_valued_double", NumericUtils.doubleToSortableLong(value)));
      }

      for (byte[] value : docValues.bytesValues) {
        doc.add(new SortedSetDocValuesField("multi_valued_bytes", newBytesRef(value)));
      }

      w1.addDocument(doc);
      w2.addDocument(doc);
      if (random().nextDouble() < deleteChance) {
        toDelete.add(id);
      }
    }
    for (int id : toDelete) {
      w1.deleteDocuments(new Term("id", Integer.toString(id)));
      w2.deleteDocuments(new Term("id", Integer.toString(id)));
    }
    DirectoryReader r1 = DirectoryReader.open(w1);
    IndexSearcher s1 = newSearcher(r1);

    if (random().nextBoolean()) {
      int maxSegmentCount = TestUtil.nextInt(random(), 1, 5);
      if (VERBOSE) {
        System.out.println("TEST: now forceMerge(" + maxSegmentCount + ")");
      }
      w2.forceMerge(maxSegmentCount);
    }

    DirectoryReader r2 = DirectoryReader.open(w2);
    IndexSearcher s2 = newSearcher(r2);

    /*
    System.out.println("TEST: full index:");
    SortedDocValues docValues = MultiDocValues.getSortedValues(r2, "bytes");
    for(int i=0;i<r2.maxDoc();i++) {
      System.out.println("  doc " + i + " id=" + r2.storedFields().document(i).get("id") + " bytes=" + docValues.get(i));
    }
    */

    for (int iter = 0; iter < 100; iter++) {
      int numHits = TestUtil.nextInt(random(), 1, numDocs);
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " numHits=" + numHits);
      }

      TopDocs hits1 =
          s1.search(
              new MatchAllDocsQuery(),
              new TopFieldCollectorManager(sort, numHits, Integer.MAX_VALUE));
      TopDocs hits2 =
          s2.search(new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, numHits, 1));

      if (VERBOSE) {
        System.out.println("  topDocs query-time sort: totalHits=" + hits1.totalHits.value);
        for (ScoreDoc scoreDoc : hits1.scoreDocs) {
          System.out.println("    " + scoreDoc.doc);
        }
        System.out.println("  topDocs index-time sort: totalHits=" + hits2.totalHits.value);
        for (ScoreDoc scoreDoc : hits2.scoreDocs) {
          System.out.println("    " + scoreDoc.doc);
        }
      }

      assertEquals(hits2.scoreDocs.length, hits1.scoreDocs.length);
      StoredFields storedFields1 = r1.storedFields();
      StoredFields storedFields2 = r2.storedFields();
      for (int i = 0; i < hits2.scoreDocs.length; i++) {
        ScoreDoc hit1 = hits1.scoreDocs[i];
        ScoreDoc hit2 = hits2.scoreDocs[i];
        assertEquals(
            storedFields1.document(hit1.doc).get("id"), storedFields2.document(hit2.doc).get("id"));
        assertArrayEquals(((FieldDoc) hit1).fields, ((FieldDoc) hit2).fields);
      }
    }

    IOUtils.close(r1, r2, w1, w2, dir1, dir2);
  }

  public void testTieBreak() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(new Sort(new SortField("foo", SortField.Type.STRING)));
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    for (int id = 0; id < 1000; id++) {
      Document doc = new Document();
      doc.add(new StoredField("id", id));
      String value;
      if (id < 500) {
        value = "bar2";
      } else {
        value = "bar1";
      }
      doc.add(new SortedDocValuesField("foo", newBytesRef(value)));
      w.addDocument(doc);
      if (id == 500) {
        w.commit();
      }
    }
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    StoredFields storedFields = r.storedFields();
    for (int docID = 0; docID < 1000; docID++) {
      int expectedID;
      if (docID < 500) {
        expectedID = 500 + docID;
      } else {
        expectedID = docID - 500;
      }
      assertEquals(
          expectedID, storedFields.document(docID).getField("id").numericValue().intValue());
    }
    IOUtils.close(r, w, dir);
  }

  public void testIndexSortWithSparseField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField = new SortField("dense_int", SortField.Type.INT, true);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    Field textField = newTextField("sparse_text", "", Field.Store.NO);
    for (int i = 0; i < 128; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("dense_int", i));
      if (i < 64) {
        doc.add(new NumericDocValuesField("sparse_int", i));
        doc.add(new BinaryDocValuesField("sparse_binary", newBytesRef(Integer.toString(i))));
        textField.setStringValue("foo");
        doc.add(textField);
      }
      w.addDocument(doc);
    }
    w.commit();
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    LeafReader leafReader = r.leaves().get(0).reader();

    NumericDocValues denseValues = leafReader.getNumericDocValues("dense_int");
    NumericDocValues sparseValues = leafReader.getNumericDocValues("sparse_int");
    BinaryDocValues sparseBinaryValues = leafReader.getBinaryDocValues("sparse_binary");
    NumericDocValues normsValues = leafReader.getNormValues("sparse_text");
    for (int docID = 0; docID < 128; docID++) {
      assertTrue(denseValues.advanceExact(docID));
      assertEquals(127 - docID, (int) denseValues.longValue());
      if (docID >= 64) {
        assertTrue(denseValues.advanceExact(docID));
        assertTrue(sparseValues.advanceExact(docID));
        assertTrue(sparseBinaryValues.advanceExact(docID));
        assertTrue(normsValues.advanceExact(docID));
        assertEquals(1, normsValues.longValue());
        assertEquals(127 - docID, (int) sparseValues.longValue());
        assertEquals(newBytesRef(Integer.toString(127 - docID)), sparseBinaryValues.binaryValue());
      } else {
        assertFalse(sparseBinaryValues.advanceExact(docID));
        assertFalse(sparseValues.advanceExact(docID));
        assertFalse(normsValues.advanceExact(docID));
      }
    }
    IOUtils.close(r, w, dir);
  }

  public void testIndexSortOnSparseField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField = new SortField("sparse", SortField.Type.INT, false);
    sortField.setMissingValue(Integer.MIN_VALUE);
    Sort indexSort = new Sort(sortField);
    iwc.setIndexSort(indexSort);
    IndexWriter w = new IndexWriter(dir, iwc);
    for (int i = 0; i < 128; i++) {
      Document doc = new Document();
      if (i < 64) {
        doc.add(new NumericDocValuesField("sparse", i));
      }
      w.addDocument(doc);
    }
    w.commit();
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    LeafReader leafReader = r.leaves().get(0).reader();
    NumericDocValues sparseValues = leafReader.getNumericDocValues("sparse");
    for (int docID = 0; docID < 128; docID++) {
      if (docID >= 64) {
        assertTrue(sparseValues.advanceExact(docID));
        assertEquals(docID - 64, (int) sparseValues.longValue());
      } else {
        assertFalse(sparseValues.advanceExact(docID));
      }
    }
    IOUtils.close(r, w, dir);
  }

  public void testWrongSortFieldType() throws Exception {
    Directory dir = newDirectory();
    List<Field> dvs = new ArrayList<>();
    dvs.add(new SortedDocValuesField("field", newBytesRef("")));
    dvs.add(new SortedSetDocValuesField("field", newBytesRef("")));
    dvs.add(new NumericDocValuesField("field", 42));
    dvs.add(new SortedNumericDocValuesField("field", 42));

    List<SortField> sortFields = new ArrayList<>();
    sortFields.add(new SortField("field", SortField.Type.STRING));
    sortFields.add(new SortedSetSortField("field", false));
    sortFields.add(new SortField("field", SortField.Type.INT));
    sortFields.add(new SortedNumericSortField("field", SortField.Type.INT));

    for (int i = 0; i < sortFields.size(); i++) {
      for (int j = 0; j < dvs.size(); j++) {
        if (i == j) {
          continue;
        }
        Sort indexSort = new Sort(sortFields.get(i));
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setIndexSort(indexSort);
        IndexWriter w = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(dvs.get(j));
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
        assertThat(exc.getMessage(), containsString("expected field [field] to be "));
        doc.clear();
        doc.add(dvs.get(i));
        w.addDocument(doc);
        doc.add(dvs.get(j));
        exc = expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
        assertEquals(
            "Inconsistency of field data structures across documents for field [field] of doc [2]. doc values type: expected '"
                + dvs.get(i).fieldType().docValuesType()
                + "', but it has '"
                + dvs.get(j).fieldType().docValuesType()
                + "'.",
            exc.getMessage());
        w.rollback();
        IOUtils.close(w);
      }
    }
    IOUtils.close(dir);
  }

  public void testDeleteByTermOrQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setIndexSort(new Sort(new SortField("numeric", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);
    Document doc = new Document();
    int numDocs = random().nextInt(2000) + 5;
    long[] expectedValues = new long[numDocs];

    for (int i = 0; i < numDocs; i++) {
      expectedValues[i] = random().nextInt(Integer.MAX_VALUE);
      doc.clear();
      doc.add(new StringField("id", Integer.toString(i), Store.YES));
      doc.add(new NumericDocValuesField("numeric", expectedValues[i]));
      w.addDocument(doc);
    }
    int numDeleted = random().nextInt(numDocs) + 1;
    for (int i = 0; i < numDeleted; i++) {
      int idToDelete = random().nextInt(numDocs);
      if (random().nextBoolean()) {
        w.deleteDocuments(new TermQuery(new Term("id", Integer.toString(idToDelete))));
      } else {
        w.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
      }

      expectedValues[idToDelete] = -random().nextInt(Integer.MAX_VALUE); // force a reordering
      doc.clear();
      doc.add(new StringField("id", Integer.toString(idToDelete), Store.YES));
      doc.add(new NumericDocValuesField("numeric", expectedValues[idToDelete]));
      w.addDocument(doc);
    }

    int docCount = 0;
    try (IndexReader reader = DirectoryReader.open(w)) {
      for (LeafReaderContext leafCtx : reader.leaves()) {
        final Bits liveDocs = leafCtx.reader().getLiveDocs();
        final NumericDocValues values = leafCtx.reader().getNumericDocValues("numeric");
        if (values == null) {
          continue;
        }
        StoredFields storedFields = leafCtx.reader().storedFields();
        for (int id = 0; id < leafCtx.reader().maxDoc(); id++) {
          if (liveDocs != null && liveDocs.get(id) == false) {
            continue;
          }
          if (values.advanceExact(id) == false) {
            continue;
          }
          int globalId = Integer.parseInt(storedFields.document(id).getField("id").stringValue());
          assertTrue(values.advanceExact(id));
          assertEquals(expectedValues[globalId], values.longValue());
          docCount++;
        }
      }
      assertEquals(docCount, numDocs);
    }
    w.close();
    dir.close();
  }

  public void testSortDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);
    Document doc = new Document();
    NumericDocValuesField sort = new NumericDocValuesField("sort", 0L);
    doc.add(sort);
    StringField field = new StringField("field", "a", Field.Store.NO);
    doc.add(field);
    w.addDocument(doc);
    sort.setLongValue(1);
    field.setStringValue("b");
    w.addDocument(doc);
    sort.setLongValue(-1);
    field.setStringValue("a");
    w.addDocument(doc);
    sort.setLongValue(2);
    field.setStringValue("a");
    w.addDocument(doc);
    sort.setLongValue(3);
    field.setStringValue("b");
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leafReader = getOnlyLeafReader(reader);
    TermsEnum fieldTerms = leafReader.terms("field").iterator();
    assertEquals(new BytesRef("a"), fieldTerms.next());
    PostingsEnum postings = fieldTerms.postings(null, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.nextDoc());
    assertEquals(3, postings.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertEquals(new BytesRef("b"), fieldTerms.next());
    postings = fieldTerms.postings(postings, PostingsEnum.ALL);
    assertEquals(2, postings.nextDoc());
    assertEquals(4, postings.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertNull(fieldTerms.next());
    reader.close();
    dir.close();
  }

  public void testSortDocsAndFreqs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setTokenized(false);
    ft.freeze();
    Document doc = new Document();
    doc.add(new NumericDocValuesField("sort", 0L));
    doc.add(new Field("field", "a", ft));
    doc.add(new Field("field", "a", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 1L));
    doc.add(new Field("field", "b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", -1L));
    doc.add(new Field("field", "a", ft));
    doc.add(new Field("field", "a", ft));
    doc.add(new Field("field", "a", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 2L));
    doc.add(new Field("field", "a", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 3L));
    doc.add(new Field("field", "b", ft));
    doc.add(new Field("field", "b", ft));
    doc.add(new Field("field", "b", ft));
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leafReader = getOnlyLeafReader(reader);
    TermsEnum fieldTerms = leafReader.terms("field").iterator();
    assertEquals(new BytesRef("a"), fieldTerms.next());
    PostingsEnum postings = fieldTerms.postings(null, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(3, postings.freq());
    assertEquals(1, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(3, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertEquals(new BytesRef("b"), fieldTerms.next());
    postings = fieldTerms.postings(postings, PostingsEnum.ALL);
    assertEquals(2, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(4, postings.nextDoc());
    assertEquals(3, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertNull(fieldTerms.next());
    reader.close();
    dir.close();
  }

  public void testSortDocsAndFreqsAndPositions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    ft.freeze();
    Document doc = new Document();
    doc.add(new NumericDocValuesField("sort", 0L));
    doc.add(new Field("field", "a a b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 1L));
    doc.add(new Field("field", "b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", -1L));
    doc.add(new Field("field", "b a b b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 2L));
    doc.add(new Field("field", "a", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 3L));
    doc.add(new Field("field", "b b", ft));
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leafReader = getOnlyLeafReader(reader);
    TermsEnum fieldTerms = leafReader.terms("field").iterator();
    assertEquals(new BytesRef("a"), fieldTerms.next());
    PostingsEnum postings = fieldTerms.postings(null, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(1, postings.nextPosition());
    assertEquals(1, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(1, postings.nextPosition());
    assertEquals(3, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertEquals(new BytesRef("b"), fieldTerms.next());
    postings = fieldTerms.postings(postings, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(3, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(2, postings.nextPosition());
    assertEquals(3, postings.nextPosition());
    assertEquals(1, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(2, postings.nextPosition());
    assertEquals(2, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(4, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(1, postings.nextPosition());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertNull(fieldTerms.next());
    reader.close();
    dir.close();
  }

  public void testSortDocsAndFreqsAndPositionsAndOffsets() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
    config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    IndexWriter w = new IndexWriter(dir, config);
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    ft.setTokenized(true);
    ft.freeze();
    Document doc = new Document();
    doc.add(new NumericDocValuesField("sort", 0L));
    doc.add(new Field("field", "a a b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 1L));
    doc.add(new Field("field", "b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", -1L));
    doc.add(new Field("field", "b a b b", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 2L));
    doc.add(new Field("field", "a", ft));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("sort", 3L));
    doc.add(new Field("field", "b b", ft));
    w.addDocument(doc);
    w.forceMerge(1);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leafReader = getOnlyLeafReader(reader);
    TermsEnum fieldTerms = leafReader.terms("field").iterator();
    assertEquals(new BytesRef("a"), fieldTerms.next());
    PostingsEnum postings = fieldTerms.postings(null, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(1, postings.nextPosition());
    assertEquals(2, postings.startOffset());
    assertEquals(3, postings.endOffset());
    assertEquals(1, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(0, postings.startOffset());
    assertEquals(1, postings.endOffset());
    assertEquals(1, postings.nextPosition());
    assertEquals(2, postings.startOffset());
    assertEquals(3, postings.endOffset());
    assertEquals(3, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(0, postings.startOffset());
    assertEquals(1, postings.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertEquals(new BytesRef("b"), fieldTerms.next());
    postings = fieldTerms.postings(postings, PostingsEnum.ALL);
    assertEquals(0, postings.nextDoc());
    assertEquals(3, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(0, postings.startOffset());
    assertEquals(1, postings.endOffset());
    assertEquals(2, postings.nextPosition());
    assertEquals(4, postings.startOffset());
    assertEquals(5, postings.endOffset());
    assertEquals(3, postings.nextPosition());
    assertEquals(6, postings.startOffset());
    assertEquals(7, postings.endOffset());
    assertEquals(1, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(2, postings.nextPosition());
    assertEquals(4, postings.startOffset());
    assertEquals(5, postings.endOffset());
    assertEquals(2, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(0, postings.startOffset());
    assertEquals(1, postings.endOffset());
    assertEquals(4, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(0, postings.nextPosition());
    assertEquals(0, postings.startOffset());
    assertEquals(1, postings.endOffset());
    assertEquals(1, postings.nextPosition());
    assertEquals(2, postings.startOffset());
    assertEquals(3, postings.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    assertNull(fieldTerms.next());
    reader.close();
    dir.close();
  }

  public void testBlockContainsParentField() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      String parentField = "parent";
      iwc.setParentField(parentField);
      Sort indexSort = new Sort(new SortField("foo", SortField.Type.INT));
      iwc.setIndexSort(indexSort);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        List<Runnable> runnabels =
            Arrays.asList(
                () -> {
                  IllegalArgumentException ex =
                      expectThrows(
                          IllegalArgumentException.class,
                          () -> {
                            Document doc = new Document();
                            doc.add(new NumericDocValuesField("parent", 0));
                            writer.addDocuments(Arrays.asList(doc, new Document()));
                          });
                  assertEquals(
                      "\"parent\" is a reserved field and should not be added to any document",
                      ex.getMessage());
                },
                () -> {
                  IllegalArgumentException ex =
                      expectThrows(
                          IllegalArgumentException.class,
                          () -> {
                            Document doc = new Document();
                            doc.add(new NumericDocValuesField("parent", 0));
                            writer.addDocuments(Arrays.asList(new Document(), doc));
                          });
                  assertEquals(
                      "\"parent\" is a reserved field and should not be added to any document",
                      ex.getMessage());
                });
        Collections.shuffle(runnabels, random());
        for (Runnable runnable : runnabels) {
          runnable.run();
        }
      }
    }
  }

  public void testIndexSortWithBlocks() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      AssertingNeedsIndexSortCodec codec = new AssertingNeedsIndexSortCodec();
      iwc.setCodec(codec);
      String parentField = "parent";
      Sort indexSort = new Sort(new SortField("foo", SortField.Type.INT));
      iwc.setIndexSort(indexSort);
      iwc.setParentField(parentField);
      LogMergePolicy policy = newLogMergePolicy();
      // make sure that merge factor is always > 2
      if (policy.getMergeFactor() <= 2) {
        policy.setMergeFactor(3);
      }
      iwc.setMergePolicy(policy);

      // add already sorted documents
      codec.numCalls = 0;
      codec.needsIndexSort = false;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        int numDocs = 50 + random().nextInt(50);
        for (int i = 0; i < numDocs; i++) {
          Document child1 = new Document();
          child1.add(new StringField("id", Integer.toString(i), Store.YES));
          child1.add(new NumericDocValuesField("id", i));
          child1.add(new NumericDocValuesField("child", 1));
          child1.add(new NumericDocValuesField("foo", random().nextInt()));
          Document child2 = new Document();
          child2.add(new StringField("id", Integer.toString(i), Store.YES));
          child2.add(new NumericDocValuesField("id", i));
          child2.add(new NumericDocValuesField("child", 2));
          child2.add(new NumericDocValuesField("foo", random().nextInt()));
          Document parent = new Document();
          parent.add(new StringField("id", Integer.toString(i), Store.YES));
          parent.add(new NumericDocValuesField("id", i));
          parent.add(new NumericDocValuesField("foo", random().nextInt()));
          w.addDocuments(Arrays.asList(child1, child2, parent));
          if (rarely()) {
            w.commit();
          }
        }
        w.commit();
        if (random().nextBoolean()) {
          w.forceMerge(1, true);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leaf = ctx.reader();
          NumericDocValues parentDISI = leaf.getNumericDocValues(parentField);
          NumericDocValues ids = leaf.getNumericDocValues("id");
          NumericDocValues children = leaf.getNumericDocValues("child");
          int doc;
          int expectedDocID = 2;
          while ((doc = parentDISI.nextDoc()) != NO_MORE_DOCS) {
            assertEquals(-1, parentDISI.longValue());
            assertEquals(expectedDocID, doc);
            int id = ids.nextDoc();
            long child1ID = ids.longValue();
            assertEquals(id, children.nextDoc());
            long child1 = children.longValue();
            assertEquals(1, child1);

            id = ids.nextDoc();
            long child2ID = ids.longValue();
            assertEquals(id, children.nextDoc());
            long child2 = children.longValue();
            assertEquals(2, child2);

            int idParent = ids.nextDoc();
            assertEquals(id + 1, idParent);
            long parent = ids.longValue();
            assertEquals(child1ID, parent);
            assertEquals(child2ID, parent);
            expectedDocID += 3;
          }
        }
      }
    }
  }

  @SuppressWarnings("fallthrough")
  public void testMixRandomDocumentsWithBlocks() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      AssertingNeedsIndexSortCodec codec = new AssertingNeedsIndexSortCodec();
      iwc.setCodec(codec);
      String parentField = "parent";
      Sort indexSort = new Sort(new SortField("foo", SortField.Type.INT));
      iwc.setIndexSort(indexSort);
      iwc.setParentField(parentField);
      RandomIndexWriter randomIndexWriter = new RandomIndexWriter(random(), dir, iwc);
      int numDocs = 100 + random().nextInt(900);
      for (int i = 0; i < numDocs; i++) {
        if (rarely()) {
          randomIndexWriter.deleteDocuments(new Term("id", "" + random().nextInt(i + 1)));
        }
        List<Document> docs = new ArrayList<>();
        switch (random().nextInt(100) % 5) {
          case 4:
            Document child3 = new Document();
            child3.add(new StringField("id", Integer.toString(i), Store.YES));
            child3.add(new NumericDocValuesField("type", 2));
            child3.add(new NumericDocValuesField("child_ord", 3));
            child3.add(new NumericDocValuesField("foo", random().nextInt()));
            docs.add(child3);
          case 3:
            Document child2 = new Document();
            child2.add(new StringField("id", Integer.toString(i), Store.YES));
            child2.add(new NumericDocValuesField("type", 2));
            child2.add(new NumericDocValuesField("child_ord", 2));
            child2.add(new NumericDocValuesField("foo", random().nextInt()));
            docs.add(child2);
          case 2:
            Document child1 = new Document();
            child1.add(new StringField("id", Integer.toString(i), Store.YES));
            child1.add(new NumericDocValuesField("type", 2));
            child1.add(new NumericDocValuesField("child_ord", 1));
            child1.add(new NumericDocValuesField("foo", random().nextInt()));
            docs.add(child1);
          case 1:
            Document root = new Document();
            root.add(new StringField("id", Integer.toString(i), Store.YES));
            root.add(new NumericDocValuesField("type", 1));
            root.add(new NumericDocValuesField("num_children", docs.size()));
            root.add(new NumericDocValuesField("foo", random().nextInt()));
            docs.add(root);
            randomIndexWriter.addDocuments(docs);
            break;
          case 0:
            Document single = new Document();
            single.add(new StringField("id", Integer.toString(i), Store.YES));
            single.add(new NumericDocValuesField("type", 0));
            single.add(new NumericDocValuesField("foo", random().nextInt()));
            randomIndexWriter.addDocument(single);
        }
        if (rarely()) {
          randomIndexWriter.forceMerge(1);
        }
        randomIndexWriter.commit();
      }

      randomIndexWriter.close();
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leaf = ctx.reader();
          NumericDocValues parentDISI = leaf.getNumericDocValues(parentField);
          assertNotNull(parentDISI);
          NumericDocValues type = leaf.getNumericDocValues("type");
          NumericDocValues childOrd = leaf.getNumericDocValues("child_ord");
          NumericDocValues numChildren = leaf.getNumericDocValues("num_children");
          int numCurrentChildren = 0;
          int totalPendingChildren = 0;
          String childId = null;
          for (int i = 0; i < leaf.maxDoc(); i++) {
            if (leaf.getLiveDocs() == null || leaf.getLiveDocs().get(i)) {
              assertTrue(type.advanceExact(i));
              int typeValue = (int) type.longValue();
              switch (typeValue) {
                case 2:
                  assertFalse(parentDISI.advanceExact(i));
                  assertTrue(childOrd.advanceExact(i));
                  if (numCurrentChildren == 0) { // first child
                    childId = leaf.storedFields().document(i).get("id");
                    totalPendingChildren = (int) childOrd.longValue() - 1;
                  } else {
                    assertNotNull(childId);
                    assertEquals(totalPendingChildren--, childOrd.longValue());
                    assertEquals(childId, leaf.storedFields().document(i).get("id"));
                  }
                  numCurrentChildren++;
                  break;
                case 1:
                  assertTrue(parentDISI.advanceExact(i));
                  assertEquals(-1, parentDISI.longValue());
                  if (childOrd != null) {
                    assertFalse(childOrd.advanceExact(i));
                  }
                  assertTrue(numChildren.advanceExact(i));
                  assertEquals(0, totalPendingChildren);
                  assertEquals(numCurrentChildren, numChildren.longValue());
                  if (numCurrentChildren > 0) {
                    assertEquals(childId, leaf.storedFields().document(i).get("id"));
                  } else {
                    assertNull(childId);
                  }
                  numCurrentChildren = 0;
                  childId = null;
                  break;
                case 0:
                  assertEquals(-1, parentDISI.longValue());
                  assertTrue(parentDISI.advanceExact(i));
                  if (childOrd != null) {
                    assertFalse(childOrd.advanceExact(i));
                  }
                  if (numChildren != null) {
                    assertFalse(numChildren.advanceExact(i));
                  }
                  break;
                default:
                  fail();
              }
            }
          }
        }
      }
    }
  }
}
