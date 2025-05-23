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

package org.apache.lucene.sandbox.facet;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.LongValueFacetCounts;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.sandbox.facet.cutters.LongValueFacetCutter;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;

/** Tests long value facets, based on TestLongValueFacetCounts. */
public class TestLongValueFacet extends SandboxFacetTestCase {

  public void testBasic() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", l % 5));
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    LongValueFacetCutter longValuesFacetCutter = new LongValueFacetCutter("field");
    CountFacetRecorder countRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
    s.search(new MatchAllDocsQuery(), collectorManager);

    FacetResult result = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
    assertEquals(
        "dim=field path=[] value=-2147483648 childCount=6\n  0 (20)\n  1 (20)\n  2 (20)\n  3 (20)\n  "
            + "4 (20)\n  9223372036854775807 (1)\n",
        result.toString());

    FacetResult topChildrenResult =
        getTopChildren(2, "field", longValuesFacetCutter, countRecorder);
    assertEquals(
        "dim=field path=[] value=-2147483648 childCount=2\n  0 (20)\n  1 (20)\n",
        topChildrenResult.toString());

    assertFacetResult(
        getAllChildren("field", longValuesFacetCutter, countRecorder),
        "field",
        new String[0],
        6,
        -2147483648,
        new LabelAndValue("0", 20),
        new LabelAndValue("1", 20),
        new LabelAndValue("2", 20),
        new LabelAndValue("3", 20),
        new LabelAndValue("4", 20),
        new LabelAndValue("9223372036854775807", 1));

    r.close();
    d.close();
  }

  public void testOnlyBigLongs() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 3; l++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", Long.MAX_VALUE - l));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    LongValueFacetCutter longValuesFacetCutter = new LongValueFacetCutter("field");
    CountFacetRecorder countRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
    s.search(new MatchAllDocsQuery(), collectorManager);

    FacetResult result = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);

    assertFacetResult(
        getAllChildren("field", longValuesFacetCutter, countRecorder),
        "field",
        new String[0],
        3,
        -2147483648,
        new LabelAndValue("9223372036854775805", 1),
        new LabelAndValue("9223372036854775806", 1),
        new LabelAndValue("9223372036854775807", 1));

    // since we have no insight into the value order in the hashMap, we sort labels by value and
    // count in
    // ascending order in order to compare with expected results
    Arrays.sort(
        result.labelValues,
        Comparator.comparing((LabelAndValue a) -> a.label)
            .thenComparingLong(a -> a.value.longValue()));

    assertEquals(
        "dim=field path=[] value=-2147483648 childCount=3\n  9223372036854775805 (1)\n  "
            + "9223372036854775806 (1)\n  9223372036854775807 (1)\n",
        result.toString());
    r.close();
    d.close();
  }

  public void testRandomSingleValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int docCount = atLeast(1000);
    double missingChance = random().nextDouble();
    long maxValue;
    if (random().nextBoolean()) {
      maxValue = random().nextLong() & Long.MAX_VALUE;
    } else {
      maxValue = random().nextInt(1000);
    }
    if (VERBOSE) {
      System.out.println(
          "TEST: valueCount="
              + docCount
              + " valueRange=-"
              + maxValue
              + "-"
              + maxValue
              + " missingChance="
              + missingChance);
    }
    Long[] values = new Long[docCount];
    // int missingCount = 0;
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new IntPoint("id", i));
      if (random().nextDouble() > missingChance) {
        long value = TestUtil.nextLong(random(), -maxValue, maxValue);
        doc.add(new NumericDocValuesField("field", value));
        values[i] = value;
      } else {
        // missingCount++;
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(100);
    for (int iter = 0; iter < iters; iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
        System.out.println("  test all docs");
      }

      // all docs
      Map<Long, Integer> expected = new HashMap<>();
      int expectedChildCount = 0;
      for (int i = 0; i < docCount; i++) {
        if (values[i] != null) {
          Integer curCount = expected.get(values[i]);
          if (curCount == null) {
            curCount = 0;
            expectedChildCount++;
          }
          expected.put(values[i], curCount + 1);
        }
      }

      List<Map.Entry<Long, Integer>> expectedCounts = new ArrayList<>(expected.entrySet());

      // sort by value
      expectedCounts.sort(Comparator.comparingLong(Map.Entry::getKey));

      LongValueFacetCutter longValuesFacetCutter = new LongValueFacetCutter("field");
      CountFacetRecorder countRecorder = new CountFacetRecorder();
      FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
          new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
      s.search(new MatchAllDocsQuery(), collectorManager);
      /* TODO: uncomment and adjust when LongValueFacetCutter supports value sources
      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("  use value source");
        }

        if (random().nextBoolean()) {
          facetCounts =
              new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), fc);
        } else if (random().nextBoolean()) {
          facetCounts =
              new LongValueFacetCounts("field", MultiLongValuesSource.fromLongField("field"), fc);
        } else {
          facetCounts =
              new LongValueFacetCounts(
                  "field",
                  MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("field")),
                  fc);
        }
      } else { */
      if (VERBOSE) {
        System.out.println("  use doc values");
      }

      FacetResult actual = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
      assertSame(
          "all docs, sort facets by value",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // docCount - missingCount,
          actual,
          Integer.MAX_VALUE);

      // test getAllChildren
      expectedCounts.sort(
          Map.Entry.<Long, Integer>comparingByKey().thenComparingLong(Map.Entry::getValue));
      FacetResult allChildren = getAllChildren("field", longValuesFacetCutter, countRecorder);
      // sort labels by value, count in ascending order
      Arrays.sort(
          allChildren.labelValues,
          Comparator.comparing((LabelAndValue a) -> a.label)
              .thenComparingLong(a -> a.value.longValue()));
      assertSame(
          "test getAllChildren",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // docCount - missingCount,
          actual,
          Integer.MAX_VALUE);

      // sort by count
      expectedCounts.sort(
          (a, b) -> {
            int cmp = -Integer.compare(a.getValue(), b.getValue());
            if (cmp == 0) {
              // tie break by value
              cmp = Long.compare(a.getKey(), b.getKey());
            }
            return cmp;
          });
      int topN;
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = RandomNumbers.randomIntBetween(random(), 1, docCount - 1);
      }
      if (VERBOSE) {
        System.out.println("  topN=" + topN);
      }
      actual = getTopChildren(topN, "field", longValuesFacetCutter, countRecorder);
      assertSame(
          "all docs, sort facets by count",
          expectedCounts,
          Math.min(topN, expectedChildCount),
          // expectedChildCount,
          -2147483648,
          // docCount - missingCount,
          actual,
          topN);

      // subset of docs
      int minId = RandomNumbers.randomIntBetween(random(), 0, docCount - 1);
      int maxId = RandomNumbers.randomIntBetween(random(), 0, docCount - 1);
      if (minId > maxId) {
        int tmp = minId;
        minId = maxId;
        maxId = tmp;
      }
      if (VERBOSE) {
        System.out.println("  test id range " + minId + "-" + maxId);
      }

      longValuesFacetCutter = new LongValueFacetCutter("field");
      countRecorder = new CountFacetRecorder();
      collectorManager = new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
      s.search(IntPoint.newRangeQuery("id", minId, maxId), collectorManager);
      // TODO: uncomment and change longValuesFacetCutter when LongValueFacetCutter supports value
      // sources
      // if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("  use doc values");
      }
      /*} else {
        if (VERBOSE) {
          System.out.println("  use value source");
        }
        if (random().nextBoolean()) {
          facetCounts =
              new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), fc);
        } else if (random().nextBoolean()) {
          facetCounts =
              new LongValueFacetCounts("field", MultiLongValuesSource.fromLongField("field"), fc);
        } else {
          facetCounts =
              new LongValueFacetCounts(
                  "field",
                  MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("field")),
                  fc);
        }
      }*/

      expected = new HashMap<>();
      expectedChildCount = 0;
      // int totCount = 0;
      for (int i = minId; i <= maxId; i++) {
        if (values[i] != null) {
          // totCount++;
          Integer curCount = expected.get(values[i]);
          if (curCount == null) {
            expectedChildCount++;
            curCount = 0;
          }
          expected.put(values[i], curCount + 1);
        }
      }
      expectedCounts = new ArrayList<>(expected.entrySet());

      // sort by value
      expectedCounts.sort(Comparator.comparingLong(Map.Entry::getKey));
      actual = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
      assertSame(
          "id " + minId + "-" + maxId + ", sort facets by value",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // totCount,
          actual,
          Integer.MAX_VALUE);

      // sort by count
      expectedCounts.sort(
          (a, b) -> {
            int cmp = -Integer.compare(a.getValue(), b.getValue());
            if (cmp == 0) {
              // tie break by value
              cmp = Long.compare(a.getKey(), b.getKey());
            }
            return cmp;
          });
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = RandomNumbers.randomIntBetween(random(), 1, docCount - 1);
      }
      actual = getTopChildren(topN, "field", longValuesFacetCutter, countRecorder);
      assertSame(
          "id " + minId + "-" + maxId + ", sort facets by count",
          expectedCounts,
          Math.min(topN, expectedChildCount),
          // expectedChildCount,
          -2147483648,
          // totCount,
          actual,
          topN);
    }
    r.close();
    dir.close();
  }

  public void testRandomMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int docCount = atLeast(1000);
    double missingChance = random().nextDouble();

    // sometimes exercise codec optimizations when a claimed multi valued field is in fact single
    // valued:
    boolean allSingleValued = rarely();
    long maxValue;

    if (random().nextBoolean()) {
      maxValue = random().nextLong() & Long.MAX_VALUE;
    } else {
      maxValue = RandomNumbers.randomLongBetween(random(), 0, 999);
    }
    if (VERBOSE) {
      System.out.println(
          "TEST: valueCount="
              + docCount
              + " valueRange=-"
              + maxValue
              + "-"
              + maxValue
              + " missingChance="
              + missingChance
              + " allSingleValued="
              + allSingleValued);
    }

    long[][] values = new long[docCount][];
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new IntPoint("id", i));
      if (random().nextDouble() > missingChance) {
        if (allSingleValued) {
          values[i] = new long[1];
        } else {
          values[i] = new long[TestUtil.nextInt(random(), 1, 5)];
        }

        for (int j = 0; j < values[i].length; j++) {
          long value = TestUtil.nextLong(random(), -maxValue, maxValue);
          values[i][j] = value;
          doc.add(new SortedNumericDocValuesField("field", value));
        }

        if (VERBOSE) {
          System.out.println("  doc=" + i + " values=" + Arrays.toString(values[i]));
        }

        // sort values to enable duplicate detection by comparing with the previous value
        Arrays.sort(values[i]);
      } else {
        if (VERBOSE) {
          System.out.println("  doc=" + i + " missing values");
        }
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(100);
    for (int iter = 0; iter < iters; iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
        System.out.println("  test all docs");
      }

      // all docs
      Map<Long, Integer> expected = new HashMap<>();
      // int expectedTotalCount = 0;
      for (int i = 0; i < docCount; i++) {
        if (values[i] != null && values[i].length > 0) {
          // expectedTotalCount++;
          setExpectedFrequencies(values[i], expected);
        }
      }

      List<Map.Entry<Long, Integer>> expectedCounts = new ArrayList<>(expected.entrySet());
      int expectedChildCount = expected.size();

      // sort by value
      expectedCounts.sort(Comparator.comparingLong(Map.Entry::getKey));

      LongValueFacetCutter longValuesFacetCutter = new LongValueFacetCutter("field");
      CountFacetRecorder countRecorder = new CountFacetRecorder();
      FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
          new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
      s.search(new MatchAllDocsQuery(), collectorManager);
      if (VERBOSE) {
        System.out.println("  use doc values");
      }
      // TODO: uncomment and adjust when LongValueFacetCutter supports value sources
      /*if (random().nextBoolean()) {
        facetCounts = new LongValueFacetCounts("field", fc);
      } else {
        facetCounts =
            new LongValueFacetCounts("field", MultiLongValuesSource.fromLongField("field"), fc);
      }*/

      FacetResult actual = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
      assertSame(
          "all docs, sort facets by value",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // expectedTotalCount,
          actual,
          Integer.MAX_VALUE);

      // test getAllChildren
      expectedCounts.sort(
          Map.Entry.<Long, Integer>comparingByKey().thenComparingLong(Map.Entry::getValue));
      FacetResult allChildren = getAllChildren("field", longValuesFacetCutter, countRecorder);
      // sort labels by value, count in ascending order
      Arrays.sort(
          allChildren.labelValues,
          Comparator.comparing((LabelAndValue a) -> a.label)
              .thenComparingLong(a -> a.value.longValue()));
      assertSame(
          "test getAllChildren",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // expectedTotalCount,
          actual,
          Integer.MAX_VALUE);

      // sort by count
      expectedCounts.sort(
          (a, b) -> {
            int cmp = -Integer.compare(a.getValue(), b.getValue());
            if (cmp == 0) {
              // tie break by value
              cmp = Long.compare(a.getKey(), b.getKey());
            }
            return cmp;
          });
      int topN;
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = RandomNumbers.randomIntBetween(random(), 1, docCount - 1);
      }
      if (VERBOSE) {
        System.out.println("  topN=" + topN);
      }
      actual = getTopChildren(topN, "field", longValuesFacetCutter, countRecorder);
      assertSame(
          "all docs, sort facets by count",
          expectedCounts,
          Math.min(topN, expectedChildCount),
          // expectedChildCount,
          -2147483648,
          // expectedTotalCount,
          actual,
          topN);

      // subset of docs
      int minId = RandomNumbers.randomIntBetween(random(), 0, docCount - 1);
      int maxId = RandomNumbers.randomIntBetween(random(), 0, docCount - 1);
      if (minId > maxId) {
        int tmp = minId;
        minId = maxId;
        maxId = tmp;
      }
      if (VERBOSE) {
        System.out.println("  test id range " + minId + "-" + maxId);
      }

      longValuesFacetCutter = new LongValueFacetCutter("field");
      countRecorder = new CountFacetRecorder();
      collectorManager = new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
      s.search(IntPoint.newRangeQuery("id", minId, maxId), collectorManager);
      // TODO: uncomment and adjust when LongValueFacetCutter supports value sources
      /*if (random().nextBoolean()) {
        facetCounts = new LongValueFacetCounts("field", fc);
      } else {
        facetCounts =
            new LongValueFacetCounts("field", MultiLongValuesSource.fromLongField("field"), fc);
      }*/

      expected = new HashMap<>();
      // expectedTotalCount = 0;
      for (int i = minId; i <= maxId; i++) {
        if (values[i] != null && values[i].length > 0) {
          // expectedTotalCount++;
          setExpectedFrequencies(values[i], expected);
        }
      }
      expectedCounts = new ArrayList<>(expected.entrySet());
      expectedChildCount = expected.size();

      // sort by value
      expectedCounts.sort(Comparator.comparingLong(Map.Entry::getKey));
      actual = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
      assertSame(
          "id " + minId + "-" + maxId + ", sort facets by value",
          expectedCounts,
          expectedChildCount,
          -2147483648,
          // expectedTotalCount,
          actual,
          Integer.MAX_VALUE);

      // sort by count
      expectedCounts.sort(
          (a, b) -> {
            int cmp = -Integer.compare(a.getValue(), b.getValue());
            if (cmp == 0) {
              // tie break by value
              cmp = Long.compare(a.getKey(), b.getKey());
            }
            return cmp;
          });
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = RandomNumbers.randomIntBetween(random(), 1, docCount - 1);
      }
      actual = getTopChildren(topN, "field", longValuesFacetCutter, countRecorder);
      assertSame(
          "id " + minId + "-" + maxId + ", sort facets by count",
          expectedCounts,
          Math.min(expectedChildCount, topN),
          // expectedChildCount,
          -2147483648,
          // expectedTotalCount,
          actual,
          topN);
    }
    r.close();
    dir.close();
  }

  private void setExpectedFrequencies(long[] values, Map<Long, Integer> expected) {
    long previousValue = 0;
    for (int j = 0; j < values.length; j++) {
      if (j == 0 || previousValue != values[j]) {
        Integer curCount = expected.getOrDefault(values[j], 0);
        expected.put(values[j], curCount + 1);
      }
      previousValue = values[j];
    }
  }

  private static void assertSame(
      String desc,
      List<Map.Entry<Long, Integer>> expectedCounts,
      int expectedChildCount,
      int expectedTotalCount,
      FacetResult actual,
      int topN) {
    int expectedTopN = Math.min(topN, expectedCounts.size());
    if (VERBOSE) {
      System.out.println("  expected topN=" + expectedTopN);
      for (int i = 0; i < expectedTopN; i++) {
        System.out.println(
            "    "
                + i
                + ": value="
                + expectedCounts.get(i).getKey()
                + " count="
                + expectedCounts.get(i).getValue());
      }
      System.out.println("  actual topN=" + actual.labelValues.length);
      for (int i = 0; i < actual.labelValues.length; i++) {
        System.out.println(
            "    "
                + i
                + ": value="
                + actual.labelValues[i].label
                + " count="
                + actual.labelValues[i].value);
      }
    }
    assertEquals(desc + ": topN", expectedTopN, actual.labelValues.length);
    assertEquals(desc + ": childCount", expectedChildCount, actual.childCount);
    assertEquals(desc + ": totCount", expectedTotalCount, actual.value.intValue());
    assertTrue(actual.labelValues.length <= topN);

    for (int i = 0; i < expectedTopN; i++) {
      assertEquals(
          desc + ": label[" + i + "]",
          Long.toString(expectedCounts.get(i).getKey()),
          actual.labelValues[i].label);
      assertEquals(
          desc + ": counts[" + i + "]",
          expectedCounts.get(i).getValue().intValue(),
          actual.labelValues[i].value.intValue());
    }
  }

  /**
   * LUCENE-9964: Duplicate long values in a document field should only be counted once when using
   * SortedNumericDocValuesFields
   */
  public void testDuplicateLongValues() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    // these two values are not unique in a document
    doc.add(new SortedNumericDocValuesField("field", 42));
    doc.add(new SortedNumericDocValuesField("field", 42));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 43));
    doc.add(new SortedNumericDocValuesField("field", 43));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    LongValueFacetCutter longValuesFacetCutter = new LongValueFacetCutter("field");
    CountFacetRecorder countRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longValuesFacetCutter, countRecorder);
    s.search(new MatchAllDocsQuery(), collectorManager);

    FacetResult fr = getAllChildrenSortByValue("field", longValuesFacetCutter, countRecorder);
    for (LabelAndValue labelAndValue : fr.labelValues) {
      assert labelAndValue.value.equals(1);
    }

    assertFacetResult(
        getAllChildren("field", longValuesFacetCutter, countRecorder),
        "field",
        new String[0],
        2,
        -2147483648,
        new LabelAndValue("42", 1),
        new LabelAndValue("43", 1));

    r.close();
    dir.close();
  }

  /**
   * Get all results sorted by value, similar to {@link
   * LongValueFacetCounts#getAllChildrenSortByValue()}
   */
  private FacetResult getAllChildrenSortByValue(
      String fieldName,
      LongValueFacetCutter longValuesFacetCutter,
      CountFacetRecorder countRecorder)
      throws IOException {
    int[] resultOrdinals = countRecorder.recordedOrds().toArray();
    ComparableSupplier<ComparableUtils.ByLongValueComparable> comparableSupplier =
        ComparableUtils.byLongValue(longValuesFacetCutter);

    ComparableUtils.sort(resultOrdinals, comparableSupplier);

    FacetLabel[] labels = longValuesFacetCutter.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(
        fieldName,
        new String[0],
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }

  /**
   * Get top results sorted by count with tie-break by value, similar to {@link
   * LongValueFacetCounts#getTopChildren(int, String, String...)}
   */
  private FacetResult getTopChildren(
      int topN,
      String field,
      LongValueFacetCutter longValuesFacetCutter,
      CountFacetRecorder countRecorder)
      throws IOException {
    ComparableSupplier<ComparableUtils.ByCountAndLongValueComparable> comparableSupplier =
        ComparableUtils.byCount(countRecorder, longValuesFacetCutter);

    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(countRecorder.recordedOrds(), comparableSupplier, topN);

    int[] resultOrdinals = topByCountOrds.toArray();

    FacetLabel[] labels = longValuesFacetCutter.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(
        field,
        new String[0],
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }

  /**
   * Get all results in no particular order, similar to {@link
   * LongValueFacetCounts#getAllChildren(String, String...)}
   */
  private FacetResult getAllChildren(
      String field, LongValueFacetCutter longValuesFacetCutter, CountFacetRecorder countRecorder)
      throws IOException {
    int[] resultOrdinals = countRecorder.recordedOrds().toArray();

    FacetLabel[] labels = longValuesFacetCutter.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    int childCount = 0;
    for (int i = 0; i < resultOrdinals.length; i++) {
      int count = countRecorder.getCount(resultOrdinals[i]);
      labelsAndValues.add(new LabelAndValue(labels[i].lastComponent(), count));
      childCount++;
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return new FacetResult(
        field,
        new String[0],
        VALUE_CANT_BE_COMPUTED,
        labelsAndValues.toArray(new LabelAndValue[0]),
        childCount);
  }
}
