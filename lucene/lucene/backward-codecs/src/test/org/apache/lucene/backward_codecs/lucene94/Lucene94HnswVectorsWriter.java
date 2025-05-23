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

package org.apache.lucene.backward_codecs.lucene94;

import static org.apache.lucene.backward_codecs.lucene94.Lucene94RWHnswVectorsFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes vector values and knn graphs to index segments.
 *
 * @lucene.experimental
 */
public final class Lucene94HnswVectorsWriter extends KnnVectorsWriter {

  private final SegmentWriteState segmentWriteState;
  private final IndexOutput meta, vectorData, vectorIndex;
  private final int M;
  private final int beamWidth;

  private final List<FieldWriter<?>> fields = new ArrayList<>();
  private boolean finished;

  Lucene94HnswVectorsWriter(SegmentWriteState state, int M, int beamWidth) throws IOException {
    this.M = M;
    this.beamWidth = beamWidth;
    segmentWriteState = state;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene94HnswVectorsFormat.META_EXTENSION);

    String vectorDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene94HnswVectorsFormat.VECTOR_DATA_EXTENSION);

    String indexDataFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene94HnswVectorsFormat.VECTOR_INDEX_EXTENSION);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      vectorData = state.directory.createOutput(vectorDataFileName, state.context);
      vectorIndex = state.directory.createOutput(indexDataFileName, state.context);

      CodecUtil.writeIndexHeader(
          meta,
          Lucene94HnswVectorsFormat.META_CODEC_NAME,
          Lucene94HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorData,
          Lucene94HnswVectorsFormat.VECTOR_DATA_CODEC_NAME,
          Lucene94HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          vectorIndex,
          Lucene94HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME,
          Lucene94HnswVectorsFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter<?> newField =
        FieldWriter.create(fieldInfo, M, beamWidth, segmentWriteState.infoStream);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter<?> field : fields) {
      if (sortMap == null) {
        writeField(field, maxDoc);
      } else {
        writeSortingField(field, maxDoc, sortMap);
      }
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (vectorData != null) {
      CodecUtil.writeFooter(vectorData);
      CodecUtil.writeFooter(vectorIndex);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  private void writeField(FieldWriter<?> fieldData, int maxDoc) throws IOException {
    // write vector values
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        writeByteVectors(fieldData);
        break;
      default:
      case FLOAT32:
        writeFloat32Vectors(fieldData);
    }
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    // write graph
    long vectorIndexOffset = vectorIndex.getFilePointer();
    OnHeapHnswGraph graph = fieldData.getGraph();
    writeGraph(graph);
    long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        fieldData.docsWithField,
        graph);
  }

  private void writeFloat32Vectors(FieldWriter<?> fieldData) throws IOException {
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (Object v : fieldData.vectors) {
      buffer.asFloatBuffer().put((float[]) v);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  private void writeByteVectors(FieldWriter<?> fieldData) throws IOException {
    for (Object v : fieldData.vectors) {
      byte[] vector = (byte[]) v;
      vectorData.writeBytes(vector, vector.length);
    }
  }

  private void writeSortingField(FieldWriter<?> fieldData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {
    final int[] docIdOffsets = new int[sortMap.size()];
    int offset = 1; // 0 means no vector for this (field, document)
    DocIdSetIterator iterator = fieldData.docsWithField.iterator();
    for (int docID = iterator.nextDoc();
        docID != DocIdSetIterator.NO_MORE_DOCS;
        docID = iterator.nextDoc()) {
      int newDocID = sortMap.oldToNew(docID);
      docIdOffsets[newDocID] = offset++;
    }
    DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
    final int[] ordMap = new int[offset - 1]; // new ord to old ord
    final int[] oldOrdMap = new int[offset - 1]; // old ord to new ord
    int ord = 0;
    int doc = 0;
    for (int docIdOffset : docIdOffsets) {
      if (docIdOffset != 0) {
        ordMap[ord] = docIdOffset - 1;
        oldOrdMap[docIdOffset - 1] = ord;
        newDocsWithField.add(doc);
        ord++;
      }
      doc++;
    }

    // write vector values
    long vectorDataOffset;
    switch (fieldData.fieldInfo.getVectorEncoding()) {
      case BYTE:
        vectorDataOffset = writeSortedByteVectors(fieldData, ordMap);
        break;
      default:
      case FLOAT32:
        vectorDataOffset = writeSortedFloat32Vectors(fieldData, ordMap);
        break;
    }
    ;
    long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;

    // write graph
    long vectorIndexOffset = vectorIndex.getFilePointer();
    OnHeapHnswGraph graph = fieldData.getGraph();
    HnswGraph mockGraph = reconstructAndWriteGraph(graph, ordMap, oldOrdMap);
    long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;

    writeMeta(
        fieldData.fieldInfo,
        maxDoc,
        vectorDataOffset,
        vectorDataLength,
        vectorIndexOffset,
        vectorIndexLength,
        newDocsWithField,
        mockGraph);
  }

  private long writeSortedFloat32Vectors(FieldWriter<?> fieldData, int[] ordMap)
      throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int ordinal : ordMap) {
      float[] vector = (float[]) fieldData.vectors.get(ordinal);
      buffer.asFloatBuffer().put(vector);
      vectorData.writeBytes(buffer.array(), buffer.array().length);
    }
    return vectorDataOffset;
  }

  private long writeSortedByteVectors(FieldWriter<?> fieldData, int[] ordMap) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    for (int ordinal : ordMap) {
      byte[] vector = (byte[]) fieldData.vectors.get(ordinal);
      vectorData.writeBytes(vector, vector.length);
    }
    return vectorDataOffset;
  }

  // reconstruct graph substituting old ordinals with new ordinals
  private HnswGraph reconstructAndWriteGraph(
      OnHeapHnswGraph graph, int[] newToOldMap, int[] oldToNewMap) throws IOException {
    if (graph == null) return null;

    List<int[]> nodesByLevel = new ArrayList<>(graph.numLevels());
    nodesByLevel.add(null);

    int maxOrd = graph.size();
    int maxConnOnLevel = M * 2;
    NodesIterator nodesOnLevel0 = graph.getNodesOnLevel(0);
    while (nodesOnLevel0.hasNext()) {
      int node = nodesOnLevel0.nextInt();
      NeighborArray neighbors = graph.getNeighbors(0, newToOldMap[node]);
      reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxConnOnLevel, maxOrd);
    }

    maxConnOnLevel = M;
    for (int level = 1; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      int[] newNodes = new int[nodesOnLevel.size()];
      int n = 0;
      while (nodesOnLevel.hasNext()) {
        newNodes[n++] = oldToNewMap[nodesOnLevel.nextInt()];
      }
      Arrays.sort(newNodes);
      nodesByLevel.add(newNodes);
      for (int node : newNodes) {
        NeighborArray neighbors = graph.getNeighbors(level, newToOldMap[node]);
        reconstructAndWriteNeigbours(neighbors, oldToNewMap, maxConnOnLevel, maxOrd);
      }
    }
    return new HnswGraph() {
      @Override
      public int nextNeighbor() {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public void seek(int level, int target) {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public int size() {
        return graph.size();
      }

      @Override
      public int numLevels() {
        return graph.numLevels();
      }

      @Override
      public int entryNode() {
        throw new UnsupportedOperationException("Not supported on a mock graph");
      }

      @Override
      public NodesIterator getNodesOnLevel(int level) {
        if (level == 0) {
          return graph.getNodesOnLevel(0);
        } else {
          return new ArrayNodesIterator(nodesByLevel.get(level), nodesByLevel.get(level).length);
        }
      }
    };
  }

  private void reconstructAndWriteNeigbours(
      NeighborArray neighbors, int[] oldToNewMap, int maxConnOnLevel, int maxOrd)
      throws IOException {
    int size = neighbors.size();
    vectorIndex.writeInt(size);

    // Destructively modify; it's ok we are discarding it after this
    int[] nnodes = neighbors.nodes();
    for (int i = 0; i < size; i++) {
      nnodes[i] = oldToNewMap[nnodes[i]];
    }
    Arrays.sort(nnodes, 0, size);
    for (int i = 0; i < size; i++) {
      int nnode = nnodes[i];
      assert nnode < maxOrd : "node too large: " + nnode + ">=" + maxOrd;
      vectorIndex.writeInt(nnode);
    }
    // if number of connections < maxConn,
    // add bogus values up to maxConn to have predictable offsets
    for (int i = size; i < maxConnOnLevel; i++) {
      vectorIndex.writeInt(0);
    }
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
    IndexOutput tempVectorData =
        segmentWriteState.directory.createTempOutput(
            vectorData.getName(), "temp", segmentWriteState.context);
    IndexInput vectorDataInput = null;
    boolean success = false;
    try {
      // write the vector data to a temporary file
      final DocsWithFieldSet docsWithField;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          docsWithField =
              writeByteVectorData(
                  tempVectorData, MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState));
          break;
        case FLOAT32:
          docsWithField =
              writeVectorData(
                  tempVectorData, MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState));
          break;
        default:
          throw new IllegalArgumentException(
              "unknown vector encoding=" + fieldInfo.getVectorEncoding());
      }
      CodecUtil.writeFooter(tempVectorData);
      IOUtils.close(tempVectorData);

      // copy the temporary file vectors to the actual data file
      vectorDataInput =
          segmentWriteState.directory.openInput(
              tempVectorData.getName(), segmentWriteState.context);
      vectorData.copyBytes(vectorDataInput, vectorDataInput.length() - CodecUtil.footerLength());
      CodecUtil.retrieveChecksum(vectorDataInput);
      long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
      long vectorIndexOffset = vectorIndex.getFilePointer();
      // build the graph using the temporary vector data
      // we use Lucene94HnswVectorsReader.DenseOffHeapVectorValues for the graph construction
      // doesn't need to know docIds
      // TODO: separate random access vector values from DocIdSetIterator?
      int byteSize = fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize;
      OnHeapHnswGraph graph = null;
      DefaultFlatVectorScorer defaultFlatVectorScorer = new DefaultFlatVectorScorer();
      if (docsWithField.cardinality() != 0) {
        // build graph
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE:
            OffHeapByteVectorValues.DenseOffHeapVectorValues bytesValues =
                new OffHeapByteVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    vectorDataInput,
                    fieldInfo.getVectorSimilarityFunction(),
                    byteSize);
            RandomVectorScorerSupplier scorerBytesSupplier =
                defaultFlatVectorScorer.getRandomVectorScorerSupplier(
                    fieldInfo.getVectorSimilarityFunction(), bytesValues);
            HnswGraphBuilder bytesGraphBuilder =
                HnswGraphBuilder.create(
                    scorerBytesSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
            bytesGraphBuilder.setInfoStream(segmentWriteState.infoStream);
            graph = bytesGraphBuilder.build(bytesValues.size());
            break;

          case FLOAT32:
            OffHeapFloatVectorValues.DenseOffHeapVectorValues vectorValues =
                new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality(),
                    vectorDataInput,
                    fieldInfo.getVectorSimilarityFunction(),
                    byteSize);
            RandomVectorScorerSupplier scorerSupplier =
                defaultFlatVectorScorer.getRandomVectorScorerSupplier(
                    fieldInfo.getVectorSimilarityFunction(), vectorValues);
            HnswGraphBuilder hnswGraphBuilder =
                HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
            graph = hnswGraphBuilder.build(vectorValues.size());
            break;

          default:
            throw new IllegalArgumentException(
                "unknown vector encoding=" + fieldInfo.getVectorEncoding());
        }
        writeGraph(graph);
      }
      long vectorIndexLength = vectorIndex.getFilePointer() - vectorIndexOffset;
      writeMeta(
          fieldInfo,
          segmentWriteState.segmentInfo.maxDoc(),
          vectorDataOffset,
          vectorDataLength,
          vectorIndexOffset,
          vectorIndexLength,
          docsWithField,
          graph);
      success = true;
    } finally {
      IOUtils.close(vectorDataInput);
      if (success) {
        segmentWriteState.directory.deleteFile(tempVectorData.getName());
      } else {
        IOUtils.closeWhileHandlingException(tempVectorData);
        IOUtils.deleteFilesIgnoringExceptions(
            segmentWriteState.directory, tempVectorData.getName());
      }
    }
  }

  private void writeGraph(OnHeapHnswGraph graph) throws IOException {
    if (graph == null) return;
    // write vectors' neighbours on each level into the vectorIndex file
    int countOnLevel0 = graph.size();
    for (int level = 0; level < graph.numLevels(); level++) {
      int maxConnOnLevel = level == 0 ? (M * 2) : M;
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        NeighborArray neighbors = graph.getNeighbors(level, node);
        int size = neighbors.size();
        vectorIndex.writeInt(size);
        // Destructively modify; it's ok we are discarding it after this
        int[] nnodes = neighbors.nodes();
        Arrays.sort(nnodes, 0, size);
        for (int i = 0; i < size; i++) {
          int nnode = nnodes[i];
          assert nnode < countOnLevel0 : "node too large: " + nnode + ">=" + countOnLevel0;
          vectorIndex.writeInt(nnode);
        }
        // if number of connections < maxConn, add bogus values up to maxConn to have predictable
        // offsets
        for (int i = size; i < maxConnOnLevel; i++) {
          vectorIndex.writeInt(0);
        }
      }
    }
  }

  private void writeMeta(
      FieldInfo field,
      int maxDoc,
      long vectorDataOffset,
      long vectorDataLength,
      long vectorIndexOffset,
      long vectorIndexLength,
      DocsWithFieldSet docsWithField,
      HnswGraph graph)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(field.getVectorSimilarityFunction().ordinal());
    meta.writeVLong(vectorDataOffset);
    meta.writeVLong(vectorDataLength);
    meta.writeVLong(vectorIndexOffset);
    meta.writeVLong(vectorIndexLength);
    meta.writeInt(field.getVectorDimension());

    // write docIDs
    int count = docsWithField.cardinality();
    meta.writeInt(count);
    if (count == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (count == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = vectorData.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(
              docsWithField.iterator(), vectorData, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(vectorData.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);

      // write ordToDoc mapping
      long start = vectorData.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      // dense case and empty case do not need to store ordToMap mapping
      final DirectMonotonicWriter ordToDocWriter =
          DirectMonotonicWriter.getInstance(meta, vectorData, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
      DocIdSetIterator iterator = docsWithField.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        ordToDocWriter.add(doc);
      }
      ordToDocWriter.finish();
      meta.writeLong(vectorData.getFilePointer() - start);
    }

    meta.writeInt(M);
    // write graph nodes on each level
    if (graph == null) {
      meta.writeInt(0);
    } else {
      meta.writeInt(graph.numLevels());
      for (int level = 0; level < graph.numLevels(); level++) {
        NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
        meta.writeInt(nodesOnLevel.size()); // number of nodes on a level
        if (level > 0) {
          while (nodesOnLevel.hasNext()) {
            int node = nodesOnLevel.nextInt();
            meta.writeInt(node); // list of nodes on a level
          }
        }
      }
    }
  }

  /**
   * Writes the byte vector values to the output and returns a set of documents that contains
   * vectors.
   */
  private static DocsWithFieldSet writeByteVectorData(
      IndexOutput output, ByteVectorValues byteVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    for (int docV = byteVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = byteVectorValues.nextDoc()) {
      // write vector
      byte[] binaryValue = byteVectorValues.vectorValue();
      assert binaryValue.length == byteVectorValues.dimension() * VectorEncoding.BYTE.byteSize;
      output.writeBytes(binaryValue, binaryValue.length);
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  /**
   * Writes the vector values to the output and returns a set of documents that contains vectors.
   */
  private static DocsWithFieldSet writeVectorData(
      IndexOutput output, FloatVectorValues floatVectorValues) throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    ByteBuffer binaryVector =
        ByteBuffer.allocate(floatVectorValues.dimension() * VectorEncoding.FLOAT32.byteSize)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (int docV = floatVectorValues.nextDoc();
        docV != NO_MORE_DOCS;
        docV = floatVectorValues.nextDoc()) {
      // write vector
      float[] vectorValue = floatVectorValues.vectorValue();
      binaryVector.asFloatBuffer().put(vectorValue);
      output.writeBytes(binaryVector.array(), binaryVector.limit());
      docsWithField.add(docV);
    }
    return docsWithField;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorData, vectorIndex);
  }

  private abstract static class FieldWriter<T> extends KnnFieldVectorsWriter<T> {
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<T> vectors;
    private final HnswGraphBuilder hnswGraphBuilder;

    private int lastDocID = -1;
    private int node = 0;

    static FieldWriter<?> create(FieldInfo fieldInfo, int M, int beamWidth, InfoStream infoStream)
        throws IOException {
      int dim = fieldInfo.getVectorDimension();
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          return new FieldWriter<byte[]>(fieldInfo, M, beamWidth, infoStream) {
            @Override
            public byte[] copyValue(byte[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
        default:
        case FLOAT32:
          return new FieldWriter<float[]>(fieldInfo, M, beamWidth, infoStream) {
            @Override
            public float[] copyValue(float[] value) {
              return ArrayUtil.copyOfSubArray(value, 0, dim);
            }
          };
      }
    }

    @SuppressWarnings("unchecked")
    FieldWriter(FieldInfo fieldInfo, int M, int beamWidth, InfoStream infoStream)
        throws IOException {
      this.fieldInfo = fieldInfo;
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
      DefaultFlatVectorScorer defaultFlatVectorScorer = new DefaultFlatVectorScorer();
      final RandomVectorScorerSupplier scorerSupplier;
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          scorerSupplier =
              defaultFlatVectorScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(),
                  RandomAccessVectorValues.fromBytes((List<byte[]>) vectors, dim));
          break;

        case FLOAT32:
          scorerSupplier =
              defaultFlatVectorScorer.getRandomVectorScorerSupplier(
                  fieldInfo.getVectorSimilarityFunction(),
                  RandomAccessVectorValues.fromFloats((List<float[]>) vectors, dim));
          break;

        default:
          throw new IllegalArgumentException(
              "unknown vector encoding=" + fieldInfo.getVectorEncoding());
      }
      hnswGraphBuilder =
          HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed);
      hnswGraphBuilder.setInfoStream(infoStream);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int docID, Object value) throws IOException {
      if (docID == lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      T vectorValue = (T) value;
      assert docID > lastDocID;
      docsWithField.add(docID);
      vectors.add(copyValue(vectorValue));
      hnswGraphBuilder.addGraphNode(node);
      node++;
      lastDocID = docID;
    }

    OnHeapHnswGraph getGraph() throws IOException {
      if (vectors.size() > 0) {
        return hnswGraphBuilder.getCompletedGraph();
      } else {
        return null;
      }
    }

    @Override
    public long ramBytesUsed() {
      if (vectors.size() == 0) return 0;
      long vectorSize = vectors.size();
      return docsWithField.ramBytesUsed()
          + vectorSize
              * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER)
          + vectorSize * fieldInfo.getVectorDimension() * fieldInfo.getVectorEncoding().byteSize
          + hnswGraphBuilder.getGraph().ramBytesUsed();
    }
  }
}
