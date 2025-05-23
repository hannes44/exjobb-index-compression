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
package org.apache.lucene.codecs.lucene101;

import static org.apache.lucene.codecs.lucene101.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat.*;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionFactory;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionUtils;
import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressor;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.internal.vectorization.VectorizationProvider;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.VectorUtil;

/**
 * Concrete class that reads docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * @lucene.experimental
 */
public final class CustomLucene101PostingsReader extends PostingsReaderBase {

    static final VectorizationProvider VECTORIZATION_PROVIDER = VectorizationProvider.getInstance();
    // Dummy impacts, composed of the maximum possible term frequency and the lowest possible
    // (unsigned) norm value. This is typically used on tail blocks, which don't actually record
    // impacts as the storage overhead would not be worth any query evaluation speedup, since there's
    // less than 128 docs left to evaluate anyway.
    private static final List<Impact> DUMMY_IMPACTS =
            Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));

    private final IndexInput docIn;
    private final IndexInput posIn;
    private final IndexInput payIn;
    private final IndexInput excIn;

    private final int maxNumImpactsAtLevel0;
    private final int maxImpactNumBytesAtLevel0;
    private final int maxNumImpactsAtLevel1;
    private final int maxImpactNumBytesAtLevel1;

    HashMap<Integer, ArrayList<Integer>> exceptions;



    /** Sole constructor. */
    public CustomLucene101PostingsReader(SegmentReadState state) throws IOException {


        String metaName =
                IndexFileNames.segmentFileName(
                        state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.META_EXTENSION);
        final long expectedDocFileLength, expectedPosFileLength, expectedPayFileLength;
        ChecksumIndexInput metaIn = null;
        boolean success = false;
        int version;
        try {
            metaIn = state.directory.openChecksumInput(metaName);
            version =
                    CodecUtil.checkIndexHeader(
                            metaIn,
                            META_CODEC,
                            VERSION_START,
                            VERSION_CURRENT,
                            state.segmentInfo.getId(),
                            state.segmentSuffix);
            exceptions = IntegerCompressionUtils.decodeExceptions(metaIn);
            maxNumImpactsAtLevel0 = metaIn.readInt();
            maxImpactNumBytesAtLevel0 = metaIn.readInt();
            maxNumImpactsAtLevel1 = metaIn.readInt();
            maxImpactNumBytesAtLevel1 = metaIn.readInt();
            expectedDocFileLength = metaIn.readLong();
            if (state.fieldInfos.hasProx()) {
                expectedPosFileLength = metaIn.readLong();
                if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
                    expectedPayFileLength = metaIn.readLong();
                } else {
                    expectedPayFileLength = -1;
                }
            } else {
                expectedPosFileLength = -1;
                expectedPayFileLength = -1;
            }
            CodecUtil.checkFooter(metaIn, null);
            success = true;
        } catch (Throwable t) {
            if (metaIn != null) {
                CodecUtil.checkFooter(metaIn, t);
                throw new AssertionError("unreachable");
            } else {
                throw t;
            }
        } finally {
            if (success) {
                metaIn.close();
            } else {
                IOUtils.closeWhileHandlingException(metaIn);
            }
        }

        success = false;
        IndexInput docIn = null;
        IndexInput posIn = null;
        IndexInput payIn = null;
        IndexInput excIn = null;


        // NOTE: these data files are too costly to verify checksum against all the bytes on open,
        // but for now we at least verify proper structure of the checksum footer: which looks
        // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
        // such as file truncation.

        String docName =
                IndexFileNames.segmentFileName(
                        state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.DOC_EXTENSION);
        try {
            // Postings have a forward-only access pattern, so pass ReadAdvice.NORMAL to perform
            // readahead.
            docIn = state.directory.openInput(docName, state.context.withReadAdvice(ReadAdvice.NORMAL));
            CodecUtil.checkIndexHeader(
                    docIn, DOC_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
            CodecUtil.retrieveChecksum(docIn, expectedDocFileLength);


            String excName =
                    IndexFileNames.segmentFileName(
                            state.segmentInfo.name,
                            state.segmentSuffix,
                            Lucene101PostingsFormat.EXC_EXTENSION);
            // The exception input
            if (Lucene101Codec.useExceptionFile) {
                excIn = state.directory.openInput(excName, state.context);
                CodecUtil.checkIndexHeader(
                        excIn, EXC_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);

            }

            if (state.fieldInfos.hasProx()) {
                String proxName =
                        IndexFileNames.segmentFileName(
                                state.segmentInfo.name, state.segmentSuffix, Lucene101PostingsFormat.POS_EXTENSION);
                posIn = state.directory.openInput(proxName, state.context);
                CodecUtil.checkIndexHeader(
                        posIn, POS_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
                CodecUtil.retrieveChecksum(posIn, expectedPosFileLength);

                if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
                    String payName =
                            IndexFileNames.segmentFileName(
                                    state.segmentInfo.name,
                                    state.segmentSuffix,
                                    Lucene101PostingsFormat.PAY_EXTENSION);
                    payIn = state.directory.openInput(payName, state.context);
                    CodecUtil.checkIndexHeader(
                            payIn, PAY_CODEC, version, version, state.segmentInfo.getId(), state.segmentSuffix);
                    CodecUtil.retrieveChecksum(payIn, expectedPayFileLength);
                }
            }

            this.docIn = docIn;
            this.posIn = posIn;
            this.payIn = payIn;
            this.excIn = excIn;

            if (Lucene101Codec.useExceptionFile)
                exceptions = IntegerCompressionUtils.decodeExceptions(excIn);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(docIn, posIn, payIn);
            }
        }
    }

    @Override
    public void init(IndexInput termsIn, SegmentReadState state) throws IOException {
        // Make sure we are talking to the matching postings writer
        CodecUtil.checkIndexHeader(
                termsIn,
                TERMS_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        final int indexBlockSize = termsIn.readVInt();
        if (indexBlockSize != BLOCK_SIZE) {
            throw new IllegalStateException(
                    "index-time BLOCK_SIZE ("
                            + indexBlockSize
                            + ") != read-time BLOCK_SIZE ("
                            + BLOCK_SIZE
                            + ")");
        }
    }

    static void prefixSum(int[] buffer, int count, long base) {
        buffer[0] += base;
        for (int i = 1; i < count; ++i) {
            buffer[i] += buffer[i - 1];
        }
    }

    @Override
    public BlockTermState newTermState() {
        return new IntBlockTermState();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(docIn, posIn, payIn);
    }

    @Override
    public void decodeTerm(
            DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
            throws IOException {
        final IntBlockTermState termState = (IntBlockTermState) _termState;
        if (absolute) {
            termState.docStartFP = 0;
            termState.posStartFP = 0;
            termState.payStartFP = 0;
        }

        final long l = in.readVLong();
        if ((l & 0x01) == 0) {
            termState.docStartFP += l >>> 1;
            if (termState.docFreq == 1) {
                termState.singletonDocID = in.readVInt();
            } else {
                termState.singletonDocID = -1;
            }
        } else {
            assert absolute == false;
            assert termState.singletonDocID != -1;
            termState.singletonDocID += BitUtil.zigZagDecode(l >>> 1);
        }

        if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
            termState.posStartFP += in.readVLong();
            if (fieldInfo
                    .getIndexOptions()
                    .compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
                    >= 0
                    || fieldInfo.hasPayloads()) {
                termState.payStartFP += in.readVLong();
            }
            if (termState.totalTermFreq > BLOCK_SIZE) {
                termState.lastPosBlockOffset = in.readVLong();
            } else {
                termState.lastPosBlockOffset = -1;
            }
        }
    }

    @Override
    public PostingsEnum postings(
            FieldInfo fieldInfo, BlockTermState termState, PostingsEnum reuse, int flags)
            throws IOException {
        return (reuse instanceof BlockPostingsEnum everythingEnum
                && everythingEnum.canReuse(docIn, fieldInfo, flags, false)
                ? everythingEnum
                : new BlockPostingsEnum(fieldInfo, flags, false))
                .reset((IntBlockTermState) termState, flags);
    }

    @Override
    public ImpactsEnum impacts(FieldInfo fieldInfo, BlockTermState state, int flags)
            throws IOException {
        return new BlockPostingsEnum(fieldInfo, flags, true).reset((IntBlockTermState) state, flags);
    }

    private static int sumOverRange(int[] arr, int start, int end) {
        int res = 0;
        for (int i = start; i < end; i++) {
            res += arr[i];
        }
        return res;
    }

    final class BlockPostingsEnum extends ImpactsEnum {

        private ForDeltaUtil forDeltaUtil;
        private PForUtil pforUtil;
        private IntegerCompressor integerCompressor;

        private final int[] docBuffer = new int[BLOCK_SIZE];
        private final short[] docBufferShorts = new short[BLOCK_SIZE];

        private int doc; // doc we last read

        // level 0 skip data
        private int level0LastDocID;
        private long level0DocEndFP;

        // level 1 skip data
        private int level1LastDocID;
        private long level1DocEndFP;
        private int level1DocCountUpto;

        private int docFreq; // number of docs in this posting list
        private long totalTermFreq; // sum of freqBuffer in this posting list (or docFreq when omitted)

        private int singletonDocID; // docid when there is a single pulsed posting, otherwise -1

        private int docCountLeft; // number of remaining docs in this postings list
        private int prevDocID; // last doc ID of the previous block

        private int docBufferSize;
        private int docBufferUpto;

        private IndexInput docIn;
        private PostingDecodingUtil docInUtil;

        private final int[] freqBuffer = new int[BLOCK_SIZE];
        private final int[] posDeltaBuffer;
        private final short[] posDeltaBufferShorts;
        private boolean useShorts = false;

        private final int[] payloadLengthBuffer;
        private final int[] offsetStartDeltaBuffer;
        private final int[] offsetLengthBuffer;

        private byte[] payloadBytes;
        private int payloadByteUpto;
        private int payloadLength;

        private int lastStartOffset;
        private int startOffset;
        private int endOffset;

        private int posBufferUpto;

        final IndexInput posIn;
        final PostingDecodingUtil posInUtil;
        final IndexInput payIn;
        final PostingDecodingUtil payInUtil;
        final BytesRef payload;

        final IndexOptions options;
        final boolean indexHasFreq;
        final boolean indexHasPos;
        final boolean indexHasOffsets;
        final boolean indexHasPayloads;
        final boolean indexHasOffsetsOrPayloads;

        final int flags;
        final boolean needsFreq;
        final boolean needsPos;
        final boolean needsOffsets;
        final boolean needsPayloads;
        final boolean needsOffsetsOrPayloads;
        final boolean needsImpacts;
        final boolean needsDocsAndFreqsOnly;

        private long freqFP; // offset of the freq block

        private int position; // current position

        // value of docBufferUpto on the last doc ID when positions have been read
        private int posDocBufferUpto;

        // how many positions "behind" we are; nextPosition must
        // skip these to "catch up":
        private int posPendingCount;

        // File pointer where the last (vInt encoded) pos delta
        // block is.  We need this to know whether to bulk
        // decode vs vInt decode the block:
        private long lastPosBlockFP;

        // level 0 skip data
        private long level0PosEndFP;
        private int level0BlockPosUpto;
        private long level0PayEndFP;
        private int level0BlockPayUpto;
        private final BytesRef level0SerializedImpacts;
        private final MutableImpactList level0Impacts;

        // level 1 skip data
        private long level1PosEndFP;
        private int level1BlockPosUpto;
        private long level1PayEndFP;
        private int level1BlockPayUpto;
        private final BytesRef level1SerializedImpacts;
        private final MutableImpactList level1Impacts;

        // true if we shallow-advanced to a new block that we have not decoded yet
        private boolean needsRefilling;

        public BlockPostingsEnum(FieldInfo fieldInfo, int flags, boolean needsImpacts)
                throws IOException {
            integerCompressor = IntegerCompressionFactory.CreateIntegerCompressor(Lucene101Codec.integerCompressionType);
            options = fieldInfo.getIndexOptions();
            indexHasFreq = options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
            indexHasPos = options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            indexHasOffsets =
                    options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
            indexHasPayloads = fieldInfo.hasPayloads();
            indexHasOffsetsOrPayloads = indexHasOffsets || indexHasPayloads;

            this.flags = flags;
            needsFreq = indexHasFreq && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS);
            needsPos = indexHasPos && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS);
            needsOffsets = indexHasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS);
            needsPayloads =
                    indexHasPayloads && PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS);
            needsOffsetsOrPayloads = needsOffsets || needsPayloads;
            this.needsImpacts = needsImpacts;
            needsDocsAndFreqsOnly = needsPos == false && needsImpacts == false;

            if (needsFreq == false) {
                Arrays.fill(freqBuffer, 1);
            }

            if (needsFreq && needsImpacts) {
                level0SerializedImpacts = new BytesRef(maxImpactNumBytesAtLevel0);
                level1SerializedImpacts = new BytesRef(maxImpactNumBytesAtLevel1);
                level0Impacts = new MutableImpactList(maxNumImpactsAtLevel0);
                level1Impacts = new MutableImpactList(maxNumImpactsAtLevel1);
            } else {
                level0SerializedImpacts = null;
                level1SerializedImpacts = null;
                level0Impacts = null;
                level1Impacts = null;
            }

            if (needsPos) {
                this.posIn = CustomLucene101PostingsReader.this.posIn.clone();
                posInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(posIn);
                posDeltaBuffer = new int[BLOCK_SIZE];
                posDeltaBufferShorts = new short[BLOCK_SIZE];
            } else {
                this.posIn = null;
                this.posInUtil = null;
                posDeltaBuffer = null;
                posDeltaBufferShorts = null;
            }

            if (needsOffsets || needsPayloads) {
                this.payIn = CustomLucene101PostingsReader.this.payIn.clone();
                payInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(payIn);
            } else {
                this.payIn = null;
                payInUtil = null;
            }

            if (needsOffsets) {
                offsetStartDeltaBuffer = new int[BLOCK_SIZE];
                offsetLengthBuffer = new int[BLOCK_SIZE];
            } else {
                offsetStartDeltaBuffer = null;
                offsetLengthBuffer = null;
                startOffset = -1;
                endOffset = -1;
            }

            if (indexHasPayloads) {
                payloadLengthBuffer = new int[BLOCK_SIZE];
                payloadBytes = new byte[128];
                payload = new BytesRef();
            } else {
                payloadLengthBuffer = null;
                payloadBytes = null;
                payload = null;
            }
        }

        public boolean canReuse(
                IndexInput docIn, FieldInfo fieldInfo, int flags, boolean needsImpacts) {
            return docIn == CustomLucene101PostingsReader.this.docIn
                    && options == fieldInfo.getIndexOptions()
                    && indexHasPayloads == fieldInfo.hasPayloads()
                    && this.flags == flags
                    && this.needsImpacts == needsImpacts;
        }

        public BlockPostingsEnum reset(IntBlockTermState termState, int flags) throws IOException {
            docFreq = termState.docFreq;
            singletonDocID = termState.singletonDocID;
            if (docFreq > 1) {
                if (docIn == null) {
                    // lazy init
                    docIn = CustomLucene101PostingsReader.this.docIn.clone();
                    docInUtil = VECTORIZATION_PROVIDER.newPostingDecodingUtil(docIn);
                }
                prefetchPostings(docIn, termState);
            }

            if (forDeltaUtil == null && docFreq >= BLOCK_SIZE) {
                forDeltaUtil = new ForDeltaUtil();
            }
            totalTermFreq = indexHasFreq ? termState.totalTermFreq : termState.docFreq;
            if (needsFreq && pforUtil == null && totalTermFreq >= BLOCK_SIZE) {
                pforUtil = new PForUtil();
            }

            // Where this term's postings start in the .pos file:
            final long posTermStartFP = termState.posStartFP;
            // Where this term's payloads/offsets start in the .pay
            // file:
            final long payTermStartFP = termState.payStartFP;
            if (posIn != null) {
                posIn.seek(posTermStartFP);
                if (payIn != null) {
                    payIn.seek(payTermStartFP);
                }
            }
            level1PosEndFP = posTermStartFP;
            level1PayEndFP = payTermStartFP;
            level0PosEndFP = posTermStartFP;
            level0PayEndFP = payTermStartFP;
            posPendingCount = 0;
            payloadByteUpto = 0;
            if (termState.totalTermFreq < BLOCK_SIZE) {
                lastPosBlockFP = posTermStartFP;
            } else if (termState.totalTermFreq == BLOCK_SIZE) {
                lastPosBlockFP = -1;
            } else {
                lastPosBlockFP = posTermStartFP + termState.lastPosBlockOffset;
            }

            level1BlockPosUpto = 0;
            level1BlockPayUpto = 0;
            level0BlockPosUpto = 0;
            level0BlockPayUpto = 0;
            posBufferUpto = BLOCK_SIZE;

            doc = -1;
            prevDocID = -1;
            docCountLeft = docFreq;
            freqFP = -1L;
            level0LastDocID = -1;
            if (docFreq < LEVEL1_NUM_DOCS) {
                level1LastDocID = NO_MORE_DOCS;
                if (docFreq > 1) {
                    docIn.seek(termState.docStartFP);
                }
            } else {
                level1LastDocID = -1;
                level1DocEndFP = termState.docStartFP;
            }
            level1DocCountUpto = 0;
            docBufferSize = BLOCK_SIZE;
            docBufferUpto = BLOCK_SIZE;
            posDocBufferUpto = BLOCK_SIZE;

            return this;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int freq() throws IOException {
            if (freqFP != -1) {
                docIn.seek(freqFP);
                pforUtil.decode(docInUtil, freqBuffer);
                //integerCompressor.decode(docInUtil, freqBuffer);
                freqFP = -1;
            }
            return freqBuffer[docBufferUpto - 1];
        }

        private void refillFullBlock() throws IOException {
            if (!Lucene101Codec.customEncodeDocIds)
                forDeltaUtil.decodeAndPrefixSum(docInUtil, prevDocID, docBuffer);
            else {
                boolean useShorts = integerCompressor.decode(docInUtil, docBuffer, exceptions, docBufferShorts);

                int prev = prevDocID;

                for (int i = 0; i < 128; i++) {
                    int val = useShorts ? (docBufferShorts[i] & 0xFFFF) : docBuffer[i];
                    docBuffer[i] = val + prev;
                    prev = docBuffer[i];
                }
            }



            if (indexHasFreq) {
                if (needsFreq) {
                    freqFP = docIn.getFilePointer();
                }

                PForUtil.skip(docIn);

            }
            docCountLeft -= BLOCK_SIZE;
            prevDocID = docBuffer[BLOCK_SIZE - 1];
            docBufferUpto = 0;
            posDocBufferUpto = 0;
        }

        private void refillRemainder() throws IOException {
            assert docCountLeft >= 0 && docCountLeft < BLOCK_SIZE;
            if (docFreq == 1) {
                docBuffer[0] = singletonDocID;
                freqBuffer[0] = (int) totalTermFreq;
                docBuffer[1] = NO_MORE_DOCS;
                assert freqFP == -1;
                docCountLeft = 0;
                docBufferSize = 1;
            } else {
                // Read vInts:
                PostingsUtil.readVIntBlock(
                        docIn, docBuffer, freqBuffer, docCountLeft, indexHasFreq, needsFreq);
                prefixSum(docBuffer, docCountLeft, prevDocID);
                docBuffer[docCountLeft] = NO_MORE_DOCS;
                freqFP = -1L;
                docBufferSize = docCountLeft;
                docCountLeft = 0;
            }
            prevDocID = docBuffer[BLOCK_SIZE - 1];
            docBufferUpto = 0;
            posDocBufferUpto = 0;
            assert docBuffer[docBufferSize] == NO_MORE_DOCS;
        }

        private void refillDocs() throws IOException {
            assert docCountLeft >= 0;

            if (docCountLeft >= BLOCK_SIZE) {
                refillFullBlock();
            } else {
                refillRemainder();
            }
        }

        private void skipLevel1To(int target) throws IOException {
            while (true) {
                prevDocID = level1LastDocID;
                level0LastDocID = level1LastDocID;
                docIn.seek(level1DocEndFP);
                level0PosEndFP = level1PosEndFP;
                level0BlockPosUpto = level1BlockPosUpto;
                level0PayEndFP = level1PayEndFP;
                level0BlockPayUpto = level1BlockPayUpto;
                docCountLeft = docFreq - level1DocCountUpto;
                level1DocCountUpto += LEVEL1_NUM_DOCS;

                if (docCountLeft < LEVEL1_NUM_DOCS) {
                    level1LastDocID = NO_MORE_DOCS;
                    break;
                }

                level1LastDocID += docIn.readVInt();
                long delta = docIn.readVLong();
                level1DocEndFP = delta + docIn.getFilePointer();

                if (indexHasFreq) {
                    long skip1EndFP = docIn.readShort() + docIn.getFilePointer();
                    int numImpactBytes = docIn.readShort();
                    if (needsImpacts && level1LastDocID >= target) {
                        docIn.readBytes(level1SerializedImpacts.bytes, 0, numImpactBytes);
                        level1SerializedImpacts.length = numImpactBytes;
                    } else {
                        docIn.skipBytes(numImpactBytes);
                    }
                    if (indexHasPos) {
                        level1PosEndFP += docIn.readVLong();
                        level1BlockPosUpto = docIn.readByte();
                        if (indexHasOffsetsOrPayloads) {
                            level1PayEndFP += docIn.readVLong();
                            level1BlockPayUpto = docIn.readVInt();
                        }
                    }
                    assert docIn.getFilePointer() == skip1EndFP;
                }

                if (level1LastDocID >= target) {
                    break;
                }
            }
        }

        private void doMoveToNextLevel0Block() throws IOException {
            assert docBufferUpto == BLOCK_SIZE;
            if (posIn != null) {
                if (level0PosEndFP >= posIn.getFilePointer()) {
                    posIn.seek(level0PosEndFP);
                    posPendingCount = level0BlockPosUpto;
                    if (payIn != null) {
                        assert level0PayEndFP >= payIn.getFilePointer();
                        payIn.seek(level0PayEndFP);
                        payloadByteUpto = level0BlockPayUpto;
                    }
                    posBufferUpto = BLOCK_SIZE;
                } else {
                    assert freqFP == -1L;
                    posPendingCount += sumOverRange(freqBuffer, posDocBufferUpto, BLOCK_SIZE);
                }
            }

            if (docCountLeft >= BLOCK_SIZE) {
                docIn.readVLong(); // level0NumBytes
                int docDelta = readVInt15(docIn);
                level0LastDocID += docDelta;
                long blockLength = readVLong15(docIn);
                level0DocEndFP = docIn.getFilePointer() + blockLength;
                if (indexHasFreq) {
                    int numImpactBytes = docIn.readVInt();
                    if (needsImpacts) {
                        docIn.readBytes(level0SerializedImpacts.bytes, 0, numImpactBytes);
                        level0SerializedImpacts.length = numImpactBytes;
                    } else {
                        docIn.skipBytes(numImpactBytes);
                    }

                    if (indexHasPos) {
                        level0PosEndFP += docIn.readVLong();
                        level0BlockPosUpto = docIn.readByte();
                        if (indexHasOffsetsOrPayloads) {
                            level0PayEndFP += docIn.readVLong();
                            level0BlockPayUpto = docIn.readVInt();
                        }
                    }
                }
                refillFullBlock();
            } else {
                level0LastDocID = NO_MORE_DOCS;
                refillRemainder();
            }
        }

        private void moveToNextLevel0Block() throws IOException {
            if (doc == level1LastDocID) { // advance level 1 skip data
                skipLevel1To(doc + 1);
            }

            // Now advance level 0 skip data
            prevDocID = level0LastDocID;

            if (needsDocsAndFreqsOnly && docCountLeft >= BLOCK_SIZE) {
                // Optimize the common path for exhaustive evaluation
                long level0NumBytes = docIn.readVLong();
                docIn.skipBytes(level0NumBytes);
                refillFullBlock();
                level0LastDocID = docBuffer[BLOCK_SIZE - 1];
            } else {
                doMoveToNextLevel0Block();
            }
        }

        private void readLevel0PosData() throws IOException {
            level0PosEndFP += docIn.readVLong();
            level0BlockPosUpto = docIn.readByte();
            if (indexHasOffsetsOrPayloads) {
                level0PayEndFP += docIn.readVLong();
                level0BlockPayUpto = docIn.readVInt();
            }
        }

        private void seekPosData(long posFP, int posUpto, long payFP, int payUpto) throws IOException {
            // If nextBlockPosFP is less than the current FP, it means that the block of positions for
            // the first docs of the next block are already decoded. In this case we just accumulate
            // frequencies into posPendingCount instead of seeking backwards and decoding the same pos
            // block again.
            if (posFP >= posIn.getFilePointer()) {
                posIn.seek(posFP);
                posPendingCount = posUpto;
                if (payIn != null) { // needs payloads or offsets
                    assert level0PayEndFP >= payIn.getFilePointer();
                    payIn.seek(payFP);
                    payloadByteUpto = payUpto;
                }
                posBufferUpto = BLOCK_SIZE;
            } else {
                posPendingCount += sumOverRange(freqBuffer, posDocBufferUpto, BLOCK_SIZE);
            }
        }

        private void skipLevel0To(int target) throws IOException {
            long posFP;
            int posUpto;
            long payFP;
            int payUpto;

            while (true) {
                prevDocID = level0LastDocID;

                posFP = level0PosEndFP;
                posUpto = level0BlockPosUpto;
                payFP = level0PayEndFP;
                payUpto = level0BlockPayUpto;

                if (docCountLeft >= BLOCK_SIZE) {
                    long numSkipBytes = docIn.readVLong();
                    long skip0End = docIn.getFilePointer() + numSkipBytes;
                    int docDelta = readVInt15(docIn);
                    level0LastDocID += docDelta;
                    boolean found = target <= level0LastDocID;
                    long blockLength = readVLong15(docIn);
                    level0DocEndFP = docIn.getFilePointer() + blockLength;

                    if (indexHasFreq) {
                        if (found == false && needsPos == false) {
                            docIn.seek(skip0End);
                        } else {
                            int numImpactBytes = docIn.readVInt();
                            if (needsImpacts && found) {
                                docIn.readBytes(level0SerializedImpacts.bytes, 0, numImpactBytes);
                                level0SerializedImpacts.length = numImpactBytes;
                            } else {
                                docIn.skipBytes(numImpactBytes);
                            }

                            if (needsPos) {
                                readLevel0PosData();
                            } else {
                                docIn.seek(skip0End);
                            }
                        }
                    }

                    if (found) {
                        break;
                    }

                    docIn.seek(level0DocEndFP);
                    docCountLeft -= BLOCK_SIZE;
                } else {
                    level0LastDocID = NO_MORE_DOCS;
                    break;
                }
            }

            if (posIn != null) { // needs positions
                seekPosData(posFP, posUpto, payFP, payUpto);
            }
        }

        @Override
        public void advanceShallow(int target) throws IOException {
            if (target > level0LastDocID) { // advance level 0 skip data
                doAdvanceShallow(target);

                // If we are on the last doc ID of a block and we are advancing on the doc ID just beyond
                // this block, then we decode the block. This may not be necessary, but this helps avoid
                // having to check whether we are in a block that is not decoded yet in #nextDoc().
                if (docBufferUpto == BLOCK_SIZE && target == doc + 1) {
                    refillDocs();
                    needsRefilling = false;
                } else {
                    needsRefilling = true;
                }
            }
        }

        private void doAdvanceShallow(int target) throws IOException {
            if (target > level1LastDocID) { // advance skip data on level 1
                skipLevel1To(target);
            } else if (needsRefilling) {
                docIn.seek(level0DocEndFP);
                docCountLeft -= BLOCK_SIZE;
            }

            skipLevel0To(target);
        }

        @Override
        public int nextDoc() throws IOException {
            if (docBufferUpto == BLOCK_SIZE) {
                moveToNextLevel0Block();
            }

            return this.doc = docBuffer[docBufferUpto++];
        }

        @Override
        public int advance(int target) throws IOException {
            if (target > level0LastDocID || needsRefilling) {
                if (target > level0LastDocID) {
                    doAdvanceShallow(target);
                }
                refillDocs();
                needsRefilling = false;
            }

            int next = VectorUtil.findNextGEQ(docBuffer, target, docBufferUpto, docBufferSize);
            this.doc = docBuffer[next];
            docBufferUpto = next + 1;
            return doc;
        }

        private void skipPositions(int freq) throws IOException {
            // Skip positions now:
            int toSkip = posPendingCount - freq;
            // if (DEBUG) {
            //   System.out.println("      FPR.skipPositions: toSkip=" + toSkip);
            // }

            final int leftInBlock = BLOCK_SIZE - posBufferUpto;
            if (toSkip < leftInBlock) {
                int end = posBufferUpto + toSkip;
                if (needsPayloads) {
                    payloadByteUpto += sumOverRange(payloadLengthBuffer, posBufferUpto, end);
                }
                posBufferUpto = end;
            } else {
                toSkip -= leftInBlock;
                while (toSkip >= BLOCK_SIZE) {
                    assert posIn.getFilePointer() != lastPosBlockFP;
                    //PForUtil.skip(posIn);
                    integerCompressor.skip(posIn);

                    if (payIn != null) {
                        if (indexHasPayloads) {
                            // Skip payloadLength block:
                            PForUtil.skip(payIn);

                            // Skip payloadBytes block:
                            int numBytes = payIn.readVInt();
                            payIn.seek(payIn.getFilePointer() + numBytes);
                        }

                        if (indexHasOffsets) {
                            PForUtil.skip(payIn);
                            PForUtil.skip(payIn);
                        }
                    }
                    toSkip -= BLOCK_SIZE;
                }
                refillPositions();
                if (needsPayloads) {
                    payloadByteUpto = sumOverRange(payloadLengthBuffer, 0, toSkip);
                }
                posBufferUpto = toSkip;
            }
        }

        private void refillLastPositionBlock() throws IOException {
            final int count = (int) (totalTermFreq % BLOCK_SIZE);
            int payloadLength = 0;
            int offsetLength = 0;
            payloadByteUpto = 0;
            for (int i = 0; i < count; i++) {
                int code = posIn.readVInt();
                if (indexHasPayloads) {
                    if ((code & 1) != 0) {
                        payloadLength = posIn.readVInt();
                    }
                    if (payloadLengthBuffer != null) { // needs payloads
                        payloadLengthBuffer[i] = payloadLength;
                        posDeltaBuffer[i] = code >>> 1;
                        if (payloadLength != 0) {
                            if (payloadByteUpto + payloadLength > payloadBytes.length) {
                                payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payloadLength);
                            }
                            posIn.readBytes(payloadBytes, payloadByteUpto, payloadLength);
                            payloadByteUpto += payloadLength;
                        }
                    } else {
                        posIn.skipBytes(payloadLength);
                    }
                } else {
                    posDeltaBuffer[i] = code;
                    useShorts = false;
                }

                if (indexHasOffsets) {
                    int deltaCode = posIn.readVInt();
                    if ((deltaCode & 1) != 0) {
                        offsetLength = posIn.readVInt();
                    }
                    if (offsetStartDeltaBuffer != null) { // needs offsets
                        offsetStartDeltaBuffer[i] = deltaCode >>> 1;
                        offsetLengthBuffer[i] = offsetLength;
                    }
                }
            }
            payloadByteUpto = 0;
        }

        private void refillOffsetsOrPayloads() throws IOException {
            if (indexHasPayloads) {
                if (needsPayloads) {
                    pforUtil.decode(payInUtil, payloadLengthBuffer);
                    int numBytes = payIn.readVInt();

                    if (numBytes > payloadBytes.length) {
                        payloadBytes = ArrayUtil.growNoCopy(payloadBytes, numBytes);
                    }
                    payIn.readBytes(payloadBytes, 0, numBytes);
                } else if (payIn != null) { // needs offsets
                    // this works, because when writing a vint block we always force the first length to be
                    // written
                    PForUtil.skip(payIn); // skip over lengths
                    int numBytes = payIn.readVInt(); // read length of payloadBytes
                    payIn.seek(payIn.getFilePointer() + numBytes); // skip over payloadBytes
                }
                payloadByteUpto = 0;
            }

            if (indexHasOffsets) {
                if (needsOffsets) {
                    pforUtil.decode(payInUtil, offsetStartDeltaBuffer);
                    pforUtil.decode(payInUtil, offsetLengthBuffer);
                } else if (payIn != null) { // needs payloads
                    // this works, because when writing a vint block we always force the first length to be
                    // written
                    PForUtil.skip(payIn); // skip over starts
                    PForUtil.skip(payIn); // skip over lengths
                }
            }
        }

        private void refillPositions() throws IOException {
            if (posIn.getFilePointer() == lastPosBlockFP) {
                refillLastPositionBlock();
                return;
            }
            //pforUtil.decode(posInUtil, posDeltaBuffer);
            useShorts = integerCompressor.decode(posInUtil, posDeltaBuffer, exceptions, posDeltaBufferShorts);



            if (indexHasOffsetsOrPayloads) {
                refillOffsetsOrPayloads();
            }
        }

        private void accumulatePendingPositions() throws IOException {
            int freq = freq(); // trigger lazy decoding of freqs
            posPendingCount += sumOverRange(freqBuffer, posDocBufferUpto, docBufferUpto);
            posDocBufferUpto = docBufferUpto;

            assert posPendingCount > 0;

            if (posPendingCount > freq) {
                skipPositions(freq);
                posPendingCount = freq;
            }
        }

        private void accumulatePayloadAndOffsets() {
            if (needsPayloads) {
                payloadLength = payloadLengthBuffer[posBufferUpto];
                payload.bytes = payloadBytes;
                payload.offset = payloadByteUpto;
                payload.length = payloadLength;
                payloadByteUpto += payloadLength;
            }

            if (needsOffsets) {
                startOffset = lastStartOffset + offsetStartDeltaBuffer[posBufferUpto];
                endOffset = startOffset + offsetLengthBuffer[posBufferUpto];
                lastStartOffset = startOffset;
            }
        }

        @Override
        public int nextPosition() throws IOException {
            if (needsPos == false) {
                return -1;
            }

            assert posDocBufferUpto <= docBufferUpto;
            if (posDocBufferUpto != docBufferUpto) {
                // First position we're reading on this doc
                accumulatePendingPositions();
                position = 0;
                lastStartOffset = 0;
            }

            if (posBufferUpto == BLOCK_SIZE) {
                refillPositions();
                posBufferUpto = 0;
            }
            if (!useShorts)
                position += posDeltaBuffer[posBufferUpto];
            else
                position += posDeltaBufferShorts[posBufferUpto] & 0xFFFF;

            if (needsOffsetsOrPayloads) {
                accumulatePayloadAndOffsets();
            }

            posBufferUpto++;
            posPendingCount--;
            return position;
        }

        @Override
        public int startOffset() {
            if (needsOffsets == false) {
                return -1;
            }
            return startOffset;
        }

        @Override
        public int endOffset() {
            if (needsOffsets == false) {
                return -1;
            }
            return endOffset;
        }

        @Override
        public BytesRef getPayload() {
            if (needsPayloads == false || payloadLength == 0) {
                return null;
            } else {
                return payload;
            }
        }

        @Override
        public long cost() {
            return docFreq;
        }

        private final Impacts impacts =
                new Impacts() {

                    private final ByteArrayDataInput scratch = new ByteArrayDataInput();

                    @Override
                    public int numLevels() {
                        return indexHasFreq == false || level1LastDocID == NO_MORE_DOCS ? 1 : 2;
                    }

                    @Override
                    public int getDocIdUpTo(int level) {
                        if (indexHasFreq == false) {
                            return NO_MORE_DOCS;
                        }
                        if (level == 0) {
                            return level0LastDocID;
                        }
                        return level == 1 ? level1LastDocID : NO_MORE_DOCS;
                    }

                    @Override
                    public List<Impact> getImpacts(int level) {
                        if (indexHasFreq) {
                            if (level == 0 && level0LastDocID != NO_MORE_DOCS) {
                                return readImpacts(level0SerializedImpacts, level0Impacts);
                            }
                            if (level == 1) {
                                return readImpacts(level1SerializedImpacts, level1Impacts);
                            }
                        }
                        return DUMMY_IMPACTS;
                    }

                    private List<Impact> readImpacts(BytesRef serialized, MutableImpactList impactsList) {
                        var scratch = this.scratch;
                        scratch.reset(serialized.bytes, 0, serialized.length);
                        CustomLucene101PostingsReader.readImpacts(scratch, impactsList);
                        return impactsList;
                    }
                };

        @Override
        public Impacts getImpacts() {
            assert needsImpacts;
            return impacts;
        }
    }

    /**
     * @see Lucene101PostingsWriter#writeVInt15(org.apache.lucene.store.DataOutput, int)
     */
    static int readVInt15(DataInput in) throws IOException {
        short s = in.readShort();
        if (s >= 0) {
            return s;
        } else {
            return (s & 0x7FFF) | (in.readVInt() << 15);
        }
    }

    /**
     * @see Lucene101PostingsWriter#writeVLong15(org.apache.lucene.store.DataOutput, long)
     */
    static long readVLong15(DataInput in) throws IOException {
        short s = in.readShort();
        if (s >= 0) {
            return s;
        } else {
            return (s & 0x7FFFL) | (in.readVLong() << 15);
        }
    }

    private static void prefetchPostings(IndexInput docIn, IntBlockTermState state)
            throws IOException {
        assert state.docFreq > 1; // Singletons are inlined in the terms dict, nothing to prefetch
        if (docIn.getFilePointer() != state.docStartFP) {
            // Don't prefetch if the input is already positioned at the right offset, which suggests that
            // the caller is streaming the entire inverted index (e.g. for merging), let the read-ahead
            // logic do its work instead. Note that this heuristic doesn't work for terms that have skip
            // data, since skip data is stored after the last term, but handling all terms that have <128
            // docs is a good start already.
            docIn.prefetch(state.docStartFP, 1);
        }
        // Note: we don't prefetch positions or offsets, which are less likely to be needed.
    }

    static class MutableImpactList extends AbstractList<Impact> implements RandomAccess {
        int length;
        final Impact[] impacts;

        MutableImpactList(int capacity) {
            impacts = new Impact[capacity];
            for (int i = 0; i < capacity; ++i) {
                impacts[i] = new Impact(Integer.MAX_VALUE, 1L);
            }
        }

        @Override
        public Impact get(int index) {
            return impacts[index];
        }

        @Override
        public int size() {
            return length;
        }
    }

    static MutableImpactList readImpacts(ByteArrayDataInput in, MutableImpactList reuse) {
        int freq = 0;
        long norm = 0;
        int length = 0;
        while (in.getPosition() < in.length()) {
            int freqDelta = in.readVInt();
            if ((freqDelta & 0x01) != 0) {
                freq += 1 + (freqDelta >>> 1);
                try {
                    norm += 1 + in.readZLong();
                } catch (IOException e) {
                    throw new RuntimeException(e); // cannot happen on a BADI
                }
            } else {
                freq += 1 + (freqDelta >>> 1);
                norm++;
            }
            Impact impact = reuse.impacts[length];
            impact.freq = freq;
            impact.norm = norm;
            length++;
        }
        reuse.length = length;
        return reuse;
    }

    @Override
    public void checkIntegrity() throws IOException {
        if (docIn != null) {
            CodecUtil.checksumEntireFile(docIn);
        }
        if (posIn != null) {
            CodecUtil.checksumEntireFile(posIn);
        }
        if (payIn != null) {
            CodecUtil.checksumEntireFile(payIn);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "(positions="
                + (posIn != null)
                + ",payloads="
                + (payIn != null)
                + ")";
    }
}
