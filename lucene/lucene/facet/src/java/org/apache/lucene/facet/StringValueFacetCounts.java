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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

/**
 * Compute facet counts from a previously indexed {@link SortedSetDocValues} or {@link
 * org.apache.lucene.index.SortedDocValues} field. This approach will execute facet counting against
 * the string values found in the specified field, with no assumptions on their format. Unlike
 * {@link org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts}, no assumption is made
 * about a "dimension" path component being indexed. Because of this, the field itself is
 * effectively treated as the "dimension", and counts for all unique string values are produced.
 * This approach is meant to complement {@link LongValueFacetCounts} in that they both provide facet
 * counting on a doc value field with no assumptions of content.
 *
 * <p>This implementation is useful if you want to dynamically count against any string doc value
 * field without relying on {@link FacetField} and {@link FacetsConfig}. The disadvantage is that a
 * separate field is required for each "dimension". If you want to pack multiple dimensions into the
 * same doc values field, you probably want one of {@link
 * org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts} or {@link
 * org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts}.
 *
 * <p>Note that there is an added cost on every {@link IndexReader} open to create a new {@link
 * StringDocValuesReaderState}. Also note that this class should be instantiated and used from a
 * single thread, because it holds a thread-private instance of {@link SortedSetDocValues}.
 *
 * @lucene.experimental
 */
// TODO: Add a concurrent version much like ConcurrentSortedSetDocValuesFacetCounts?
public class StringValueFacetCounts extends Facets {

  private final IndexReader reader;
  private final String field;
  private final OrdinalMap ordinalMap;
  private final SortedSetDocValues docValues;

  private int[] denseCounts;
  private final IntIntHashMap sparseCounts;
  private boolean initialized;

  private final int cardinality;
  private int totalDocCount;

  /**
   * Returns all facet counts for the field, same result as searching on {@link MatchAllDocsQuery}
   * but faster.
   */
  public StringValueFacetCounts(StringDocValuesReaderState state) throws IOException {
    this(state, null);
  }

  /** Counts facets across the provided hits. */
  public StringValueFacetCounts(StringDocValuesReaderState state, FacetsCollector facetsCollector)
      throws IOException {
    reader = state.reader;
    field = state.field;
    ordinalMap = state.ordinalMap;
    docValues = getDocValues();

    long valueCount = docValues.getValueCount();
    if (valueCount > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "can only handle valueCount < Integer.MAX_VALUE; got " + valueCount);
    }
    cardinality = (int) valueCount;

    if (facetsCollector != null) {
      if (cardinality < 1024) { // count densely for low cardinality
        sparseCounts = null;
        denseCounts = null;
        initialized = false;
        count(facetsCollector);
      } else {
        int totalHits = 0;
        int totalDocs = 0;
        for (FacetsCollector.MatchingDocs matchingDocs : facetsCollector.getMatchingDocs()) {
          totalHits += matchingDocs.totalHits;
          totalDocs += matchingDocs.context.reader().maxDoc();
        }

        // No counting needed if there are no hits:
        if (totalHits == 0) {
          sparseCounts = null;
          denseCounts = null;
          initialized = true;
        } else {
          // If our result set is < 10% of the index, we collect sparsely (use hash map). This
          // heuristic is borrowed from IntTaxonomyFacetCounts:
          if (totalHits < totalDocs / 10) {
            sparseCounts = new IntIntHashMap();
            denseCounts = null;
            initialized = true;
          } else {
            sparseCounts = null;
            denseCounts = new int[cardinality];
            initialized = true;
          }
          count(facetsCollector);
        }
      }
    } else {
      // Since we're counting all ordinals, count densely:
      sparseCounts = null;
      denseCounts = new int[cardinality];
      initialized = true;

      countAll();
    }
  }

  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    validateDimAndPathForGetChildren(dim, path);

    List<LabelAndValue> labelValues = new ArrayList<>();

    if (sparseCounts != null) {
      for (IntIntHashMap.IntIntCursor sparseCount : sparseCounts) {
        int count = sparseCount.value;
        final BytesRef term = docValues.lookupOrd(sparseCount.key);
        labelValues.add(new LabelAndValue(term.utf8ToString(), count));
      }
    } else if (denseCounts != null) {
      for (int i = 0; i < denseCounts.length; i++) {
        int count = denseCounts[i];
        if (count != 0) {
          final BytesRef term = docValues.lookupOrd(i);
          labelValues.add(new LabelAndValue(term.utf8ToString(), count));
        }
      }
    }

    return new FacetResult(
        field,
        new String[0],
        totalDocCount,
        labelValues.toArray(new LabelAndValue[0]),
        labelValues.size());
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    validateDimAndPathForGetChildren(dim, path);

    topN = Math.min(topN, cardinality);
    TopOrdAndIntNumberQueue q = null;
    TopOrdAndIntNumberQueue.OrdAndInt reuse = null;
    int childCount = 0; // total number of labels with non-zero count

    if (sparseCounts != null) {
      for (IntIntHashMap.IntIntCursor sparseCount : sparseCounts) {
        childCount++; // every count in sparseValues should be non-zero
        int ord = sparseCount.key;
        int count = sparseCount.value;
        if (q == null) {
          // Lazy init for sparse case:
          q = new TopOrdAndIntNumberQueue(topN);
        }
        if (reuse == null) {
          reuse = (TopOrdAndIntNumberQueue.OrdAndInt) q.newOrdAndValue();
        }
        reuse.ord = ord;
        reuse.value = count;
        reuse = (TopOrdAndIntNumberQueue.OrdAndInt) q.insertWithOverflow(reuse);
      }
    } else if (denseCounts != null) {
      for (int i = 0; i < denseCounts.length; i++) {
        int count = denseCounts[i];
        if (count != 0) {
          childCount++;
          if (q == null) {
            // Lazy init for sparse case:
            q = new TopOrdAndIntNumberQueue(topN);
          }
          if (reuse == null) {
            reuse = (TopOrdAndIntNumberQueue.OrdAndInt) q.newOrdAndValue();
          }
          reuse.ord = i;
          reuse.value = count;
          reuse = (TopOrdAndIntNumberQueue.OrdAndInt) q.insertWithOverflow(reuse);
        }
      }
    }

    int resultCount = q == null ? 0 : q.size();
    LabelAndValue[] labelValues = new LabelAndValue[resultCount];
    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndIntNumberQueue.OrdAndInt ordAndValue = (TopOrdAndIntNumberQueue.OrdAndInt) q.pop();
      final BytesRef term = docValues.lookupOrd(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(term.utf8ToString(), ordAndValue.value);
    }

    return new FacetResult(field, new String[0], totalDocCount, labelValues, childCount);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    int ord = (int) docValues.lookupTerm(new BytesRef(path[0]));
    if (ord < 0) {
      return -1;
    }

    if (sparseCounts != null) {
      return sparseCounts.get(ord);
    }
    if (denseCounts != null) {
      return denseCounts[ord];
    }
    return 0;
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return Collections.singletonList(getTopChildren(topN, field));
  }

  private SortedSetDocValues getDocValues() throws IOException {

    List<LeafReaderContext> leaves = reader.leaves();
    int leafCount = leaves.size();

    if (leafCount == 0) {
      return DocValues.emptySortedSet();
    }

    if (leafCount == 1) {
      return DocValues.getSortedSet(leaves.get(0).reader(), field);
    }

    // A good bit of this logic is forked from MultiDocValues so we can re-use an ordinal map
    SortedSetDocValues[] docValues = new SortedSetDocValues[leafCount];
    int[] starts = new int[leafCount + 1];
    long cost = 0;
    for (int i = 0; i < leafCount; i++) {
      LeafReaderContext context = leaves.get(i);
      SortedSetDocValues dv = DocValues.getSortedSet(context.reader(), field);
      docValues[i] = dv;
      starts[i] = context.docBase;
      cost += dv.cost();
    }
    starts[leafCount] = reader.maxDoc();

    return new MultiDocValues.MultiSortedSetDocValues(docValues, starts, ordinalMap, cost);
  }

  private void count(FacetsCollector facetsCollector) throws IOException {

    List<FacetsCollector.MatchingDocs> matchingDocs = facetsCollector.getMatchingDocs();

    if (matchingDocs.isEmpty()) {
      return;
    }

    if (matchingDocs.size() == 1) {

      FacetsCollector.MatchingDocs hits = matchingDocs.get(0);
      if (hits.totalHits == 0) {
        return;
      }

      // Validate state before doing anything else:
      validateState(hits.context);

      // Assuming the state is valid, ordinalMap should be null since we have one segment:
      assert ordinalMap == null;

      countOneSegment(docValues, hits.context.ord, hits, null);
    } else {

      // Validate state before doing anything else. We only check the first segment since they
      // should all ladder up to the same top-level reader:
      validateState(matchingDocs.get(0).context);

      for (FacetsCollector.MatchingDocs hits : matchingDocs) {
        // Assuming the state is valid, ordinalMap should be non-null and docValues should be
        // a MultiSortedSetDocValues since we have more than one segment:
        assert ordinalMap != null;
        assert docValues instanceof MultiDocValues.MultiSortedSetDocValues;

        if (hits.totalHits == 0) {
          continue;
        }

        MultiDocValues.MultiSortedSetDocValues multiValues =
            (MultiDocValues.MultiSortedSetDocValues) docValues;

        countOneSegment(multiValues.values[hits.context.ord], hits.context.ord, hits, null);
      }
    }
  }

  private void countAll() throws IOException {

    List<LeafReaderContext> leaves = reader.leaves();
    int numLeaves = leaves.size();
    if (numLeaves == 0) {
      return;
    }

    if (numLeaves == 1) {
      // Since we have a single segment, ordinalMap should be null:
      assert ordinalMap == null;

      LeafReaderContext context = leaves.get(0);
      Bits liveDocs = context.reader().getLiveDocs();
      if (liveDocs == null) {
        countOneSegmentNHLD(docValues, context.ord);
      } else {
        countOneSegment(docValues, context.ord, null, liveDocs);
      }
    } else {
      // Since we have more than one segment, we should have a non-null ordinalMap and docValues
      // should be a MultiSortedSetDocValues instance:
      assert ordinalMap != null;
      assert docValues instanceof MultiDocValues.MultiSortedSetDocValues;

      MultiDocValues.MultiSortedSetDocValues multiValues =
          (MultiDocValues.MultiSortedSetDocValues) docValues;

      for (int i = 0; i < numLeaves; i++) {
        LeafReaderContext context = leaves.get(i);
        Bits liveDocs = context.reader().getLiveDocs();
        if (liveDocs == null) {
          countOneSegmentNHLD(multiValues.values[i], context.ord);
        } else {
          countOneSegment(multiValues.values[i], context.ord, null, liveDocs);
        }
      }
    }
  }

  private void countOneSegment(
      SortedSetDocValues multiValues,
      int segmentOrd,
      FacetsCollector.MatchingDocs hits,
      Bits liveDocs)
      throws IOException {
    if (initialized == false) {
      assert denseCounts == null && sparseCounts == null;
      // If the counters weren't initialized, we can assume the cardinality is low enough that
      // dense counting will be preferrable:
      denseCounts = new int[cardinality];
      initialized = true;
    }

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

    // Intersect hits with doc values unless we're "counting all," in which case we'll iterate
    // all doc values for this segment:
    DocIdSetIterator it;
    if (hits == null) {
      assert liveDocs != null;
      it = FacetUtils.liveDocsDISI(valuesIt, liveDocs);
    } else {
      it = ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), valuesIt));
    }

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    if (ordinalMap == null) {
      // If there's no ordinal map we don't need to map segment ordinals to globals, so counting
      // is very straight-forward:
      if (singleValues != null) {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          increment(singleValues.ordValue());
          totalDocCount++;
        }
      } else {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          boolean countedDocInTotal = false;
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            increment(term);
            if (countedDocInTotal == false) {
              totalDocCount++;
              countedDocInTotal = true;
            }
          }
        }
      }
    } else {
      // We need to map segment ordinals to globals. We have two different approaches to this
      // depending on how many hits we have to count relative to how many unique doc val ordinals
      // there are in this segment:
      final LongValues ordMap = ordinalMap.getGlobalOrds(segmentOrd);
      int segmentCardinality = (int) multiValues.getValueCount();

      if (hits != null && hits.totalHits < segmentCardinality / 10) {
        // Remap every ord to global ord as we iterate:
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            increment((int) ordMap.get(singleValues.ordValue()));
            totalDocCount++;
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            boolean countedDocInTotal = false;
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              increment((int) ordMap.get(term));
              if (countedDocInTotal == false) {
                totalDocCount++;
                countedDocInTotal = true;
              }
            }
          }
        }
      } else {
        // First count in seg-ord space.
        // At this point, we're either counting all ordinals or our heuristic suggests that
        // we expect to visit a large percentage of the unique ordinals (lots of hits relative
        // to the segment cardinality), so we count the segment densely:
        final int[] segCounts = new int[segmentCardinality];
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            segCounts[singleValues.ordValue()]++;
            totalDocCount++;
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            boolean countedDocInTotal = false;
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              segCounts[term]++;
              if (countedDocInTotal == false) {
                totalDocCount++;
                countedDocInTotal = true;
              }
            }
          }
        }

        // Then, migrate to global ords:
        for (int ord = 0; ord < segmentCardinality; ord++) {
          int count = segCounts[ord];
          if (count != 0) {
            increment((int) ordMap.get(ord), count);
          }
        }
      }
    }
  }

  // Variant of countOneSegment, that has No Hits or Live Docs
  private void countOneSegmentNHLD(SortedSetDocValues multiValues, int segmentOrd)
      throws IOException {

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);

    if (ordinalMap == null) {
      // If there's no ordinal map we don't need to map segment ordinals to globals, so counting
      // is very straight-forward:
      if (singleValues != null) {
        for (int doc = singleValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = singleValues.nextDoc()) {
          increment(singleValues.ordValue());
          totalDocCount++;
        }
      } else {
        for (int doc = multiValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = multiValues.nextDoc()) {
          boolean countedDocInTotal = false;
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            increment(term);
            if (countedDocInTotal == false) {
              totalDocCount++;
              countedDocInTotal = true;
            }
          }
        }
      }
    } else {
      // We need to map segment ordinals to globals. We have two different approaches to this
      // depending on how many hits we have to count relative to how many unique doc val ordinals
      // there are in this segment:
      final LongValues ordMap = ordinalMap.getGlobalOrds(segmentOrd);
      int segmentCardinality = (int) multiValues.getValueCount();

      // First count in seg-ord space.
      // At this point, we're either counting all ordinals or our heuristic suggests that
      // we expect to visit a large percentage of the unique ordinals (lots of hits relative
      // to the segment cardinality), so we count the segment densely:
      final int[] segCounts = new int[segmentCardinality];
      if (singleValues != null) {
        for (int doc = singleValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = singleValues.nextDoc()) {
          segCounts[singleValues.ordValue()]++;
          totalDocCount++;
        }
      } else {
        for (int doc = multiValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = multiValues.nextDoc()) {
          boolean countedDocInTotal = false;
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            segCounts[term]++;
            if (countedDocInTotal == false) {
              totalDocCount++;
              countedDocInTotal = true;
            }
          }
        }
      }

      // Then, migrate to global ords:
      for (int ord = 0; ord < segmentCardinality; ord++) {
        int count = segCounts[ord];
        if (count != 0) {
          increment((int) ordMap.get(ord), count);
        }
      }
    }
  }

  private void increment(int ordinal) {
    increment(ordinal, 1);
  }

  private void increment(int ordinal, int amount) {
    if (sparseCounts != null) {
      sparseCounts.addTo(ordinal, amount);
    } else {
      denseCounts[ordinal] += amount;
    }
  }

  private void validateState(LeafReaderContext context) {
    // LUCENE-5090: make sure the provided reader context "matches"
    // the top-level reader passed to the
    // SortedSetDocValuesReaderState, else cryptic
    // AIOOBE can happen:
    if (ReaderUtil.getTopLevelContext(context).reader() != reader) {
      throw new IllegalStateException(
          "the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
    }
  }

  private void validateDimAndPathForGetChildren(String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
  }
}
