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

package org.apache.lucene.backward_codecs.lucene90;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.SplittableRandom;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * @lucene.experimental
 */
public final class Lucene90HnswVectorsReader extends KnnVectorsReader {

  private final IntObjectHashMap<FieldEntry> fields = new IntObjectHashMap<>();
  private final IndexInput vectorData;
  private final IndexInput vectorIndex;
  private final long checksumSeed;
  private final FieldInfos fieldInfos;

  Lucene90HnswVectorsReader(SegmentReadState state) throws IOException {
    int versionMeta = readMetadata(state);
    long[] checksumRef = new long[1];
    this.fieldInfos = state.fieldInfos;
    boolean success = false;
    try {
      vectorData =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
              checksumRef);
      vectorIndex =
          openDataInput(
              state,
              versionMeta,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
              Lucene90HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
              checksumRef);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
    checksumSeed = checksumRef[0];
  }

  private int readMetadata(SegmentReadState state) throws IOException {
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene90HnswVectorsFormat.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene90HnswVectorsFormat.META_CODEC_NAME,
                Lucene90HnswVectorsFormat.VERSION_START,
                Lucene90HnswVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }
    return versionMeta;
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      long[] checksumRef)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, state.context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene90HnswVectorsFormat.VERSION_START,
              Lucene90HnswVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
            in);
      }
      checksumRef[0] = CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }

      FieldEntry fieldEntry = readField(meta, info);
      validateFieldEntry(info, fieldEntry);
      fields.put(info.number, fieldEntry);
    }
  }

  private void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }

    long numBytes = (long) fieldEntry.size() * dimension * Float.BYTES;
    if (numBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size="
              + fieldEntry.size()
              + " * dim="
              + dimension
              + " * 4 = "
              + numBytes);
    }
  }

  private VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
    int similarityFunctionId = input.readInt();
    if (similarityFunctionId < 0
        || similarityFunctionId >= VectorSimilarityFunction.values().length) {
      throw new CorruptIndexException(
          "Invalid similarity function id: " + similarityFunctionId, input);
    }
    return VectorSimilarityFunction.values()[similarityFunctionId];
  }

  private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    if (similarityFunction != info.getVectorSimilarityFunction()) {
      throw new IllegalStateException(
          "Inconsistent vector similarity function for field=\""
              + info.name
              + "\"; "
              + similarityFunction
              + " != "
              + info.getVectorSimilarityFunction());
    }
    return FieldEntry.create(input, info.getVectorSimilarityFunction());
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(vectorData);
    CodecUtil.checksumEntireFile(vectorIndex);
  }

  private FieldEntry getFieldEntry(String field) {
    final FieldInfo info = fieldInfos.fieldInfo(field);
    final FieldEntry fieldEntry;
    if (info == null || (fieldEntry = fields.get(info.number)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    return fieldEntry;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return getOffHeapVectorValues(getFieldEntry(field));
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    final FieldEntry fieldEntry = getFieldEntry(field);
    if (fieldEntry.size() == 0) {
      return;
    }

    OffHeapFloatVectorValues vectorValues = getOffHeapVectorValues(fieldEntry);
    // use a seed that is fixed for the index so we get reproducible results for the same query
    final SplittableRandom random = new SplittableRandom(checksumSeed);
    NeighborQueue results =
        Lucene90OnHeapHnswGraph.search(
            target,
            knnCollector.k(),
            knnCollector.k(),
            vectorValues,
            fieldEntry.similarityFunction,
            getGraphValues(fieldEntry),
            getAcceptOrds(acceptDocs, fieldEntry),
            knnCollector.visitLimit(),
            random);
    knnCollector.incVisitedCount(results.visitedCount());
    while (results.size() > 0) {
      int node = results.topNode();
      float minSimilarity = results.topScore();
      results.pop();
      knnCollector.collect(node, minSimilarity);
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  private OffHeapFloatVectorValues getOffHeapVectorValues(FieldEntry fieldEntry)
      throws IOException {
    IndexInput bytesSlice =
        vectorData.slice("vector-data", fieldEntry.vectorDataOffset, fieldEntry.vectorDataLength);
    return new OffHeapFloatVectorValues(
        fieldEntry.dimension, fieldEntry.ordToDoc, fieldEntry.similarityFunction, bytesSlice);
  }

  private Bits getAcceptOrds(Bits acceptDocs, FieldEntry fieldEntry) {
    if (acceptDocs == null) {
      return null;
    }
    return new Bits() {
      @Override
      public boolean get(int index) {
        return acceptDocs.get(fieldEntry.ordToDoc[index]);
      }

      @Override
      public int length() {
        return fieldEntry.ordToDoc.length;
      }
    };
  }

  private HnswGraph getGraphValues(FieldEntry entry) throws IOException {
    IndexInput bytesSlice =
        vectorIndex.slice("graph-data", entry.indexDataOffset, entry.indexDataLength);
    return new OffHeapHnswGraph(entry, bytesSlice);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, vectorIndex);
  }

  private record FieldEntry(
      VectorSimilarityFunction similarityFunction,
      long vectorDataOffset,
      long vectorDataLength,
      long indexDataOffset,
      long indexDataLength,
      int dimension,
      int[] ordToDoc,
      long[] ordOffsets) {

    static FieldEntry create(DataInput input, VectorSimilarityFunction similarityFunction)
        throws IOException {
      final var vectorDataOffset = input.readVLong();
      final var vectorDataLength = input.readVLong();
      final var indexDataOffset = input.readVLong();
      final var indexDataLength = input.readVLong();
      final var dimension = input.readInt();
      final var size = input.readInt();
      final var ordToDoc = new int[size];
      for (int i = 0; i < size; i++) {
        int doc = input.readVInt();
        ordToDoc[i] = doc;
      }
      final var ordOffsets = new long[size];
      long offset = 0;
      for (int i = 0; i < ordOffsets.length; i++) {
        offset += input.readVLong();
        ordOffsets[i] = offset;
      }
      return new FieldEntry(
          similarityFunction,
          vectorDataOffset,
          vectorDataLength,
          indexDataOffset,
          indexDataLength,
          dimension,
          ordToDoc,
          ordOffsets);
    }

    int size() {
      return ordToDoc.length;
    }
  }

  /** Read the vector values from the index input. This supports both iterated and random access. */
  static class OffHeapFloatVectorValues extends FloatVectorValues {

    final int dimension;
    final int[] ordToDoc;
    final IndexInput dataIn;

    final int byteSize;
    int lastOrd = -1;
    final float[] value;
    final VectorSimilarityFunction similarityFunction;

    OffHeapFloatVectorValues(
        int dimension,
        int[] ordToDoc,
        VectorSimilarityFunction similarityFunction,
        IndexInput dataIn) {
      this.dimension = dimension;
      this.ordToDoc = ordToDoc;
      this.dataIn = dataIn;
      this.similarityFunction = similarityFunction;

      byteSize = Float.BYTES * dimension;
      value = new float[dimension];
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return ordToDoc.length;
    }

    @Override
    public OffHeapFloatVectorValues copy() {
      return new OffHeapFloatVectorValues(dimension, ordToDoc, similarityFunction, dataIn.clone());
    }

    @Override
    public float[] vectorValue(int targetOrd) throws IOException {
      if (lastOrd == targetOrd) {
        return value;
      }
      dataIn.seek((long) targetOrd * byteSize);
      dataIn.readFloats(value, 0, value.length);
      lastOrd = targetOrd;
      return value;
    }

    @Override
    public int ordToDoc(int ord) {
      return ordToDoc[ord];
    }

    @Override
    public DocIndexIterator iterator() {
      return createSparseIterator();
    }

    @Override
    public VectorScorer scorer(float[] target) {
      if (size() == 0) {
        return null;
      }
      OffHeapFloatVectorValues values = this.copy();
      DocIndexIterator iterator = values.iterator();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return values.similarityFunction.compare(values.vectorValue(iterator.index()), target);
        }

        @Override
        public DocIndexIterator iterator() {
          return iterator;
        }
      };
    }
  }

  /** Read the nearest-neighbors graph from the index input */
  private static final class OffHeapHnswGraph extends HnswGraph {

    final FieldEntry entry;
    final IndexInput dataIn;

    int arcCount;
    int arcUpTo;
    int arc;

    OffHeapHnswGraph(FieldEntry entry, IndexInput dataIn) {
      this.entry = entry;
      this.dataIn = dataIn;
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      // unsafe; no bounds checking
      dataIn.seek(entry.ordOffsets[targetOrd]);
      arcCount = dataIn.readInt();
      arc = -1;
      arcUpTo = 0;
    }

    @Override
    public int size() {
      return entry.size();
    }

    @Override
    public int nextNeighbor() throws IOException {
      if (arcUpTo >= arcCount) {
        return NO_MORE_DOCS;
      }
      ++arcUpTo;
      arc += dataIn.readVInt();
      return arc;
    }

    @Override
    public int numLevels() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int entryNode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      throw new UnsupportedOperationException();
    }
  }
}
