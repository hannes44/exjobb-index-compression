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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * A Highlighter that can get offsets from either postings ({@link
 * IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}), term vectors ({@link
 * FieldType#setStoreTermVectorOffsets(boolean)}), or via re-analyzing text.
 *
 * <p>This highlighter treats the single original document as the whole corpus, and then scores
 * individual passages as if they were documents in this corpus. It uses a {@link BreakIterator} to
 * find passages in the text; by default it breaks using {@link
 * BreakIterator#getSentenceInstance(Locale) getSentenceInstance(Locale.ROOT)}. It then iterates in
 * parallel (merge sorting by offset) through the positions of all terms from the query, coalescing
 * those hits that occur in a single passage into a {@link Passage}, and then scores each Passage
 * using a separate {@link PassageScorer}. Passages are finally formatted into highlighted snippets
 * with a {@link PassageFormatter}.
 *
 * <p>You can customize the behavior by calling some of the setters, or by subclassing and
 * overriding some methods. Some important hooks:
 *
 * <ul>
 *   <li>{@link #getBreakIterator(String)}: Customize how the text is divided into passages.
 *   <li>{@link #getScorer(String)}: Customize how passages are ranked.
 *   <li>{@link #getFormatter(String)}: Customize how snippets are formatted.
 *   <li>{@link #getPassageSortComparator(String)}: Customize how snippets are formatted.
 * </ul>
 *
 * <p>This is thread-safe, notwithstanding the setters.
 *
 * @lucene.experimental
 */
public class UnifiedHighlighter {

  protected static final char MULTIVAL_SEP_CHAR = (char) 0;

  public static final int DEFAULT_MAX_LENGTH = 10000;

  public static final int DEFAULT_CACHE_CHARS_THRESHOLD = 524288; // ~ 1 MB (2 byte chars)

  protected static final LabelledCharArrayMatcher[] ZERO_LEN_AUTOMATA_ARRAY =
      new LabelledCharArrayMatcher[0];

  // All the private defaults will be removed once non-builder based UH is removed.
  private static final boolean DEFAULT_ENABLE_MULTI_TERM_QUERY = true;
  private static final boolean DEFAULT_ENABLE_HIGHLIGHT_PHRASES_STRICTLY = true;
  private static final boolean DEFAULT_ENABLE_WEIGHT_MATCHES = true;
  private static final boolean DEFAULT_ENABLE_RELEVANCY_OVER_SPEED = true;
  private static final Supplier<BreakIterator> DEFAULT_BREAK_ITERATOR =
      () -> BreakIterator.getSentenceInstance(Locale.ROOT);
  private static final PassageScorer DEFAULT_PASSAGE_SCORER = new PassageScorer();
  private static final PassageFormatter DEFAULT_PASSAGE_FORMATTER = new DefaultPassageFormatter();
  private static final int DEFAULT_MAX_HIGHLIGHT_PASSAGES = -1;
  private static final Comparator<Passage> DEFAULT_PASSAGE_SORT_COMPARATOR =
      Comparator.comparingInt(Passage::getStartOffset);

  protected final IndexSearcher searcher; // if null, can only use highlightWithoutSearcher

  protected final Analyzer indexAnalyzer;

  // lazy initialized with double-check locking; protected so subclass can init
  protected volatile FieldInfos fieldInfos;

  private Predicate<String> fieldMatcher;

  private final Function<String, Set<String>> maskedFieldsFunc;

  private Set<HighlightFlag> flags;

  // e.g. wildcards
  private boolean handleMultiTermQuery = DEFAULT_ENABLE_MULTI_TERM_QUERY;

  // AKA "accuracy" or "query debugging"
  private boolean highlightPhrasesStrictly = DEFAULT_ENABLE_HIGHLIGHT_PHRASES_STRICTLY;

  private boolean weightMatches = DEFAULT_ENABLE_WEIGHT_MATCHES;

  // For analysis, prefer MemoryIndexOffsetStrategy
  private boolean passageRelevancyOverSpeed = DEFAULT_ENABLE_RELEVANCY_OVER_SPEED;

  private int maxLength = DEFAULT_MAX_LENGTH;

  // BreakIterator is stateful so we use a Supplier factory method
  private Supplier<BreakIterator> breakIterator = DEFAULT_BREAK_ITERATOR;

  private PassageScorer scorer = DEFAULT_PASSAGE_SCORER;

  private PassageFormatter formatter = DEFAULT_PASSAGE_FORMATTER;

  private int maxNoHighlightPassages = DEFAULT_MAX_HIGHLIGHT_PASSAGES;

  private int cacheFieldValCharsThreshold = DEFAULT_CACHE_CHARS_THRESHOLD;

  private Comparator<Passage> passageSortComparator = DEFAULT_PASSAGE_SORT_COMPARATOR;

  /**
   * Constructs the highlighter with the given index searcher and analyzer.
   *
   * @param indexSearcher Usually required, unless {@link #highlightWithoutSearcher(String, Query,
   *     String, int)} is used, in which case this needs to be null.
   * @param indexAnalyzer Required, even if in some circumstances it isn't used.
   */
  @Deprecated
  public UnifiedHighlighter(IndexSearcher indexSearcher, Analyzer indexAnalyzer) {
    this.searcher = indexSearcher; // TODO: make non nullable
    this.indexAnalyzer =
        Objects.requireNonNull(
            indexAnalyzer,
            "indexAnalyzer is required" + " (even if in some circumstances it isn't used)");
    this.maskedFieldsFunc = null;
  }

  @Deprecated
  public void setHandleMultiTermQuery(boolean handleMtq) {
    this.handleMultiTermQuery = handleMtq;
  }

  @Deprecated
  public void setHighlightPhrasesStrictly(boolean highlightPhrasesStrictly) {
    this.highlightPhrasesStrictly = highlightPhrasesStrictly;
  }

  @Deprecated
  public void setPassageRelevancyOverSpeed(boolean passageRelevancyOverSpeed) {
    this.passageRelevancyOverSpeed = passageRelevancyOverSpeed;
  }

  @Deprecated
  public void setMaxLength(int maxLength) {
    if (maxLength < 0 || maxLength == Integer.MAX_VALUE) {
      // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
      // our sentinel in the offsets queue uses this value to terminate.
      throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
    }
    this.maxLength = maxLength;
  }

  @Deprecated
  public void setBreakIterator(Supplier<BreakIterator> breakIterator) {
    this.breakIterator = breakIterator;
  }

  @Deprecated
  public void setScorer(PassageScorer scorer) {
    this.scorer = scorer;
  }

  @Deprecated
  public void setFormatter(PassageFormatter formatter) {
    this.formatter = formatter;
  }

  @Deprecated
  public void setMaxNoHighlightPassages(int defaultMaxNoHighlightPassages) {
    this.maxNoHighlightPassages = defaultMaxNoHighlightPassages;
  }

  @Deprecated
  public void setCacheFieldValCharsThreshold(int cacheFieldValCharsThreshold) {
    this.cacheFieldValCharsThreshold = cacheFieldValCharsThreshold;
  }

  @Deprecated
  public void setFieldMatcher(Predicate<String> predicate) {
    this.fieldMatcher = predicate;
  }

  @Deprecated
  public void setWeightMatches(boolean weightMatches) {
    this.weightMatches = weightMatches;
  }

  /**
   * Returns whether {@link org.apache.lucene.search.MultiTermQuery} derivatives will be
   * highlighted. By default it's enabled. MTQ highlighting can be expensive, particularly when
   * using offsets in postings.
   */
  @Deprecated
  protected boolean shouldHandleMultiTermQuery(String field) {
    return handleMultiTermQuery;
  }

  /**
   * Returns whether position sensitive queries (e.g. phrases and {@link SpanQuery}ies) should be
   * highlighted strictly based on query matches (slower) versus any/all occurrences of the
   * underlying terms. By default it's enabled, but there's no overhead if such queries aren't used.
   */
  @Deprecated
  protected boolean shouldHighlightPhrasesStrictly(String field) {
    return highlightPhrasesStrictly;
  }

  @Deprecated
  protected boolean shouldPreferPassageRelevancyOverSpeed(String field) {
    return passageRelevancyOverSpeed;
  }

  /** Builder for UnifiedHighlighter. */
  public static class Builder {
    /** If null, can only use highlightWithoutSearcher. */
    private final IndexSearcher searcher;

    private final Analyzer indexAnalyzer;
    private Predicate<String> fieldMatcher;

    private Function<String, Set<String>> maskedFieldsFunc;
    private Set<HighlightFlag> flags;
    private boolean handleMultiTermQuery = DEFAULT_ENABLE_MULTI_TERM_QUERY;
    private boolean highlightPhrasesStrictly = DEFAULT_ENABLE_HIGHLIGHT_PHRASES_STRICTLY;
    private boolean passageRelevancyOverSpeed = DEFAULT_ENABLE_RELEVANCY_OVER_SPEED;
    private boolean weightMatches = DEFAULT_ENABLE_WEIGHT_MATCHES;
    private int maxLength = DEFAULT_MAX_LENGTH;

    /** BreakIterator is stateful so we use a Supplier factory method. */
    private Supplier<BreakIterator> breakIterator = DEFAULT_BREAK_ITERATOR;

    private PassageScorer scorer = DEFAULT_PASSAGE_SCORER;
    private PassageFormatter formatter = DEFAULT_PASSAGE_FORMATTER;
    private int maxNoHighlightPassages = DEFAULT_MAX_HIGHLIGHT_PASSAGES;
    private int cacheFieldValCharsThreshold = DEFAULT_CACHE_CHARS_THRESHOLD;
    private Comparator<Passage> passageSortComparator = DEFAULT_PASSAGE_SORT_COMPARATOR;

    /**
     * Constructor for UH builder which accepts {@link IndexSearcher} and {@link Analyzer} objects.
     * {@link IndexSearcher} object can only be null when {@link #highlightWithoutSearcher(String,
     * Query, String, int)} is used.
     *
     * @param searcher - {@link IndexSearcher}
     * @param indexAnalyzer - {@link Analyzer}
     */
    public Builder(IndexSearcher searcher, Analyzer indexAnalyzer) {
      this.searcher = searcher;
      this.indexAnalyzer = indexAnalyzer;
    }

    /**
     * User-defined set of {@link HighlightFlag} values which will override the flags set by {@link
     * #withHandleMultiTermQuery(boolean)}, {@link #withHighlightPhrasesStrictly(boolean)}, {@link
     * #withPassageRelevancyOverSpeed(boolean)} and {@link #withWeightMatches(boolean)}.
     *
     * <p>Here the user can either specify the set of {@link HighlightFlag}s to be applied or use
     * the boolean flags to populate final list of {@link HighlightFlag}s.
     *
     * @param values - set of {@link HighlightFlag} values.
     */
    public Builder withFlags(Set<HighlightFlag> values) {
      this.flags = values;
      return this;
    }

    /**
     * Here position sensitive queries (e.g. phrases and {@link SpanQuery}ies) are highlighted
     * strictly based on query matches (slower) versus any/all occurrences of the underlying terms.
     * By default it's enabled, but there's no overhead if such queries aren't used.
     */
    public Builder withHighlightPhrasesStrictly(boolean value) {
      this.highlightPhrasesStrictly = value;
      return this;
    }

    /**
     * Here {@link org.apache.lucene.search.MultiTermQuery} derivatives will be highlighted. By
     * default it's enabled. MTQ highlighting can be expensive, particularly when using offsets in
     * postings.
     */
    public Builder withHandleMultiTermQuery(boolean value) {
      this.handleMultiTermQuery = value;
      return this;
    }

    /** Passage relevancy is more important than speed. True by default. */
    public Builder withPassageRelevancyOverSpeed(boolean value) {
      this.passageRelevancyOverSpeed = value;
      return this;
    }

    /**
     * Internally use the {@link Weight#matches(LeafReaderContext, int)} API for highlighting. It's
     * more accurate to the query, and the snippets can be a little different for phrases because
     * the whole phrase is marked up instead of each word. The passage relevancy calculation can be
     * different (maybe worse?) and it's slower when highlighting many fields. Use of this flag
     * requires {@link HighlightFlag#MULTI_TERM_QUERY} and {@link HighlightFlag#PHRASES} and {@link
     * HighlightFlag#PASSAGE_RELEVANCY_OVER_SPEED}. True by default because those booleans are true
     * by default.
     */
    public Builder withWeightMatches(boolean value) {
      this.weightMatches = value;
      return this;
    }

    /** The text to be highlight is effectively truncated by this length. */
    public Builder withMaxLength(int value) {
      if (value < 0 || value == Integer.MAX_VALUE) {
        // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
        // our sentinel in the offsets queue uses this value to terminate.
        throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
      }
      this.maxLength = value;
      return this;
    }

    public Builder withBreakIterator(Supplier<BreakIterator> value) {
      this.breakIterator = value;
      return this;
    }

    public Builder withFieldMatcher(Predicate<String> value) {
      this.fieldMatcher = value;
      return this;
    }

    /**
     * Set up a function that given a field retuns a set of masked fields whose matches are combined
     * to highlight the given field. Masked fields should not include the original field. This is
     * useful when you want to highlight a field based on matches from several fields.
     *
     * <p>Note: All masked fields must share the same source as the field being highlighted,
     * otherwise their offsets will not correspond to the highlighted field.
     *
     * <p>Note: Only the field being highlighted must provide an original source value (e.g. through
     * stored field), other masked fields don't need it.
     */
    public Builder withMaskedFieldsFunc(Function<String, Set<String>> maskedFieldsFunc) {
      this.maskedFieldsFunc = maskedFieldsFunc;
      return this;
    }

    public Builder withScorer(PassageScorer value) {
      this.scorer = value;
      return this;
    }

    public Builder withFormatter(PassageFormatter value) {
      this.formatter = value;
      return this;
    }

    public Builder withMaxNoHighlightPassages(int value) {
      this.maxNoHighlightPassages = value;
      return this;
    }

    public Builder withCacheFieldValCharsThreshold(int value) {
      this.cacheFieldValCharsThreshold = value;
      return this;
    }

    public Builder withPassageSortComparator(Comparator<Passage> value) {
      this.passageSortComparator = value;
      return this;
    }

    public UnifiedHighlighter build() {
      return new UnifiedHighlighter(this);
    }

    /** ... as passed in from the Builder constructor. */
    public IndexSearcher getIndexSearcher() {
      return searcher;
    }

    /** ... as passed in from the Builder constructor. */
    public Analyzer getIndexAnalyzer() {
      return indexAnalyzer;
    }

    public Set<HighlightFlag> getFlags() {
      return flags;
    }
  }

  /**
   * Creates a {@link Builder} object where {@link IndexSearcher} and {@link Analyzer} are not null.
   *
   * @param searcher - a {@link IndexSearcher} object.
   * @param indexAnalyzer - a {@link Analyzer} object.
   * @return a {@link Builder} object
   */
  public static Builder builder(IndexSearcher searcher, Analyzer indexAnalyzer) {
    return new Builder(searcher, indexAnalyzer);
  }

  /**
   * Creates a {@link Builder} object in which you can only use {@link
   * UnifiedHighlighter#highlightWithoutSearcher(String, Query, String, int)} for highlighting.
   *
   * @param indexAnalyzer - a {@link Analyzer} object.
   * @return a {@link Builder} object
   */
  public static Builder builderWithoutSearcher(Analyzer indexAnalyzer) {
    return new Builder(null, indexAnalyzer);
  }

  /**
   * Constructs the highlighter with the given {@link Builder}.
   *
   * @param builder - a {@link Builder} object.
   */
  public UnifiedHighlighter(Builder builder) {
    this.searcher = builder.searcher;
    this.indexAnalyzer =
        Objects.requireNonNull(
            builder.indexAnalyzer,
            "indexAnalyzer is required (even if in some circumstances it isn't used)");
    this.flags = evaluateFlags(builder);
    this.maxLength = builder.maxLength;
    this.breakIterator = builder.breakIterator;
    this.fieldMatcher = builder.fieldMatcher;
    this.maskedFieldsFunc = builder.maskedFieldsFunc;
    this.scorer = builder.scorer;
    this.formatter = builder.formatter;
    this.maxNoHighlightPassages = builder.maxNoHighlightPassages;
    this.cacheFieldValCharsThreshold = builder.cacheFieldValCharsThreshold;
    this.passageSortComparator = builder.passageSortComparator;
  }

  /** Extracts matching terms */
  protected static Set<Term> extractTerms(Query query) {
    Set<Term> queryTerms = new HashSet<>();
    query.visit(QueryVisitor.termCollector(queryTerms));
    return queryTerms;
  }

  /**
   * This method returns the set of of {@link HighlightFlag}s, which will be applied to the UH
   * object. The output depends on the values provided to {@link
   * Builder#withHandleMultiTermQuery(boolean)}, {@link
   * Builder#withHighlightPhrasesStrictly(boolean)}, {@link
   * Builder#withPassageRelevancyOverSpeed(boolean)} and {@link Builder#withWeightMatches(boolean)}
   * OR {@link #setHandleMultiTermQuery(boolean)}, {@link #setHighlightPhrasesStrictly(boolean)},
   * {@link #setPassageRelevancyOverSpeed(boolean)} and {@link #setWeightMatches(boolean)}
   *
   * @param shouldHandleMultiTermQuery - flag for adding Multi-term query
   * @param shouldHighlightPhrasesStrictly - flag for adding phrase highlighting
   * @param shouldPassageRelevancyOverSpeed - flag for adding passage relevancy
   * @param shouldEnableWeightMatches - flag for enabling weight matches
   * @return a set of {@link HighlightFlag}s.
   */
  protected Set<HighlightFlag> evaluateFlags(
      final boolean shouldHandleMultiTermQuery,
      final boolean shouldHighlightPhrasesStrictly,
      final boolean shouldPassageRelevancyOverSpeed,
      final boolean shouldEnableWeightMatches) {
    Set<HighlightFlag> highlightFlags = EnumSet.noneOf(HighlightFlag.class);
    if (shouldHandleMultiTermQuery) {
      highlightFlags.add(HighlightFlag.MULTI_TERM_QUERY);
    }
    if (shouldHighlightPhrasesStrictly) {
      highlightFlags.add(HighlightFlag.PHRASES);
    }
    if (shouldPassageRelevancyOverSpeed) {
      highlightFlags.add(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED);
    }

    // Evaluate if WEIGHT_MATCHES can be added as a flag.
    final boolean applyWeightMatches =
        highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY)
            && highlightFlags.contains(HighlightFlag.PHRASES)
            && highlightFlags.contains(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED)
            // User can also opt-out of WEIGHT_MATCHES.
            && shouldEnableWeightMatches;

    if (applyWeightMatches) {
      highlightFlags.add(HighlightFlag.WEIGHT_MATCHES);
    }
    return highlightFlags;
  }

  /**
   * Evaluate the highlight flags and set the {@link #flags} variable. This is called only once when
   * the Builder object is used to create a UH object.
   *
   * @param uhBuilder - {@link Builder} object.
   * @return {@link HighlightFlag}s.
   */
  protected Set<HighlightFlag> evaluateFlags(Builder uhBuilder) {
    if (flags != null) {
      return flags;
    }
    return flags =
        evaluateFlags(
            uhBuilder.handleMultiTermQuery,
            uhBuilder.highlightPhrasesStrictly,
            uhBuilder.passageRelevancyOverSpeed,
            uhBuilder.weightMatches);
  }

  /**
   * Evaluate the highlight flags and set the {@link #flags} variable. This is called every time
   * {@link #getFlags(String)} method is called. This is used in the builder and has been marked
   * deprecated since it is used only for the mutable initialization of a UH object.
   *
   * @param uh - {@link UnifiedHighlighter} object.
   * @return {@link HighlightFlag}s.
   */
  @Deprecated
  protected Set<HighlightFlag> evaluateFlags(UnifiedHighlighter uh) {
    return evaluateFlags(
        uh.handleMultiTermQuery,
        uh.highlightPhrasesStrictly,
        uh.passageRelevancyOverSpeed,
        uh.weightMatches);
  }

  /**
   * Returns the predicate to use for extracting the query part that must be highlighted. By default
   * only queries that target the current field are kept. (AKA requireFieldMatch)
   */
  protected Predicate<String> getFieldMatcher(String field) {
    if (fieldMatcher != null) {
      return fieldMatcher;
    } else {
      // requireFieldMatch = true
      return (qf) -> field.equals(qf);
    }
  }

  protected Set<String> getMaskedFields(String field) {
    return maskedFieldsFunc == null ? null : maskedFieldsFunc.apply(field);
  }

  /** Returns the {@link HighlightFlag}s applicable for the current UH instance. */
  protected Set<HighlightFlag> getFlags(String field) {
    // If a builder is used for initializing a UH object, then flags will never be null.
    // Once the setters are removed, this method can just return the flags.
    if (flags != null) {
      return flags;
    }
    // When not using builder, you have to reevaluate the flags.
    return evaluateFlags(this);
  }

  /**
   * The maximum content size to process. Content will be truncated to this size before
   * highlighting. Typically snippets closer to the beginning of the document better summarize its
   * content.
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Returns the {@link BreakIterator} to use for dividing text into passages. This returns {@link
   * BreakIterator#getSentenceInstance(Locale)} by default; subclasses can override to customize.
   *
   * <p>Note: this highlighter will call {@link BreakIterator#preceding(int)} and {@link
   * BreakIterator#next()} many times on it. The default generic JDK implementation of {@code
   * preceding} performs poorly.
   */
  protected BreakIterator getBreakIterator(String field) {
    return breakIterator.get();
  }

  /** Returns the {@link PassageScorer} to use for ranking passages. */
  protected PassageScorer getScorer(String field) {
    return scorer;
  }

  /**
   * Returns the {@link PassageFormatter} to use for formatting passages into highlighted snippets.
   */
  protected PassageFormatter getFormatter(String field) {
    return formatter;
  }

  /** Returns the {@link Comparator} to use for finally sorting passages. */
  protected Comparator<Passage> getPassageSortComparator(String field) {
    return passageSortComparator;
  }

  /**
   * Returns the number of leading passages (as delineated by the {@link BreakIterator}) when no
   * highlights could be found. If it's less than 0 (the default) then this defaults to the {@code
   * maxPassages} parameter given for each request. If this is 0 then the resulting highlight is
   * null (not formatted).
   */
  protected int getMaxNoHighlightPassages(String field) {
    return maxNoHighlightPassages;
  }

  /**
   * Limits the amount of field value pre-fetching until this threshold is passed. The highlighter
   * internally highlights in batches of documents sized on the sum field value length (in chars) of
   * the fields to be highlighted (bounded by {@link #getMaxLength()} for each field). By setting
   * this to 0, you can force documents to be fetched and highlighted one at a time, which you
   * usually shouldn't do. The default is 524288 chars which translates to about a megabyte.
   * However, note that the highlighter sometimes ignores this and highlights one document at a time
   * (without caching a bunch of documents in advance) when it can detect there's no point in it --
   * such as when all fields will be highlighted via re-analysis as one example.
   */
  public int getCacheFieldValCharsThreshold() { // question: should we size by bytes instead?
    return cacheFieldValCharsThreshold;
  }

  /** ... as passed in from constructor. */
  public IndexSearcher getIndexSearcher() {
    return searcher;
  }

  /** ... as passed in from constructor. */
  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  /** Source of term offsets; essential for highlighting. */
  public enum OffsetSource {
    POSTINGS,
    TERM_VECTORS,
    ANALYSIS,
    POSTINGS_WITH_TERM_VECTORS,
    NONE_NEEDED
  }

  /**
   * Determine the offset source for the specified field. The default algorithm is as follows:
   *
   * <ol>
   *   <li>This calls {@link #getFieldInfo(String)}. Note this returns null if there is no searcher
   *       or if the field isn't found there.
   *   <li>If there's a field info it has {@link
   *       IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS} then {@link OffsetSource#POSTINGS}
   *       is returned.
   *   <li>If there's a field info and {@link FieldInfo#hasVectors()} then {@link
   *       OffsetSource#TERM_VECTORS} is returned (note we can't check here if the TV has offsets;
   *       if there isn't then an exception will get thrown down the line).
   *   <li>Fall-back: {@link OffsetSource#ANALYSIS} is returned.
   * </ol>
   *
   * <p>Note that the highlighter sometimes switches to something else based on the query, such as
   * if you have {@link OffsetSource#POSTINGS_WITH_TERM_VECTORS} but in fact don't need term
   * vectors.
   */
  protected OffsetSource getOffsetSource(String field) {
    FieldInfo fieldInfo = getFieldInfo(field);
    if (fieldInfo != null) {
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
        return fieldInfo.hasVectors()
            ? OffsetSource.POSTINGS_WITH_TERM_VECTORS
            : OffsetSource.POSTINGS;
      }
      if (fieldInfo.hasVectors()) { // unfortunately we can't also check if the TV has offsets
        return OffsetSource.TERM_VECTORS;
      }
    }
    return OffsetSource.ANALYSIS;
  }

  /**
   * Called by the default implementation of {@link #getOffsetSource(String)}. If there is no
   * searcher then we simply always return null.
   */
  protected FieldInfo getFieldInfo(String field) {
    if (searcher == null) {
      return null;
    }
    // Need thread-safety for lazy-init but lets avoid 'synchronized' by using double-check locking
    // idiom
    FieldInfos fieldInfos = this.fieldInfos; // note: it's volatile; read once
    if (fieldInfos == null) {
      synchronized (this) {
        fieldInfos = this.fieldInfos;
        if (fieldInfos == null) {
          fieldInfos = FieldInfos.getMergedFieldInfos(searcher.getIndexReader());
          this.fieldInfos = fieldInfos;
        }
      }
    }
    return fieldInfos.fieldInfo(field);
  }

  /**
   * Highlights the top passages from a single field.
   *
   * @param field field name to highlight. Must have a stored string value and also be indexed with
   *     offsets.
   * @param query query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>. If
   *     no highlights were found for a document, the first sentence for the field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public String[] highlight(String field, Query query, TopDocs topDocs) throws IOException {
    return highlight(field, query, topDocs, 1);
  }

  /**
   * Highlights the top-N passages from a single field.
   *
   * @param field field name to highlight. Must have a stored string value.
   * @param query query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @param maxPassages The maximum number of top-N ranked passages used to form the highlighted
   *     snippets.
   * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>. If
   *     no highlights were found for a document, the first {@code maxPassages} sentences from the
   *     field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public String[] highlight(String field, Query query, TopDocs topDocs, int maxPassages)
      throws IOException {
    Map<String, String[]> res =
        highlightFields(new String[] {field}, query, topDocs, new int[] {maxPassages});
    return res.get(field);
  }

  /**
   * Highlights the top passages from multiple fields.
   *
   * <p>Conceptually, this behaves as a more efficient form of:
   *
   * <pre class="prettyprint">
   * Map m = new HashMap();
   * for (String field : fields) {
   * m.put(field, highlight(field, query, topDocs));
   * }
   * return m;
   * </pre>
   *
   * @param fields field names to highlight. Must have a stored string value.
   * @param query query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @return Map keyed on field name, containing the array of formatted snippets corresponding to
   *     the documents in <code>topDocs</code>. If no highlights were found for a document, the
   *     first sentence from the field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(String[] fields, Query query, TopDocs topDocs)
      throws IOException {
    int[] maxPassages = new int[fields.length];
    Arrays.fill(maxPassages, 1);
    return highlightFields(fields, query, topDocs, maxPassages);
  }

  /**
   * Highlights the top-N passages from multiple fields.
   *
   * <p>Conceptually, this behaves as a more efficient form of:
   *
   * <pre class="prettyprint">
   * Map m = new HashMap();
   * for (String field : fields) {
   * m.put(field, highlight(field, query, topDocs, maxPassages));
   * }
   * return m;
   * </pre>
   *
   * @param fields field names to highlight. Must have a stored string value.
   * @param query query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @param maxPassages The maximum number of top-N ranked passages per-field used to form the
   *     highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets corresponding to
   *     the documents in <code>topDocs</code>. If no highlights were found for a document, the
   *     first {@code maxPassages} sentences from the field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(
      String[] fields, Query query, TopDocs topDocs, int[] maxPassages) throws IOException {
    final ScoreDoc[] scoreDocs = topDocs.scoreDocs;
    int[] docids = new int[scoreDocs.length];
    for (int i = 0; i < docids.length; i++) {
      docids[i] = scoreDocs[i].doc;
    }

    return highlightFields(fields, query, docids, maxPassages);
  }

  /**
   * Highlights the top-N passages from multiple fields, for the provided int[] docids.
   *
   * @param fieldsIn field names to highlight. Must have a stored string value.
   * @param query query to highlight.
   * @param docidsIn containing the document IDs to highlight.
   * @param maxPassagesIn The maximum number of top-N ranked passages per-field used to form the
   *     highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets corresponding to
   *     the documents in <code>docidsIn</code>. If no highlights were found for a document, the
   *     first {@code maxPassages} from the field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(
      String[] fieldsIn, Query query, int[] docidsIn, int[] maxPassagesIn) throws IOException {
    Map<String, String[]> snippets = new HashMap<>();
    for (Map.Entry<String, Object[]> ent :
        highlightFieldsAsObjects(fieldsIn, query, docidsIn, maxPassagesIn).entrySet()) {
      Object[] snippetObjects = ent.getValue();
      String[] snippetStrings = new String[snippetObjects.length];
      snippets.put(ent.getKey(), snippetStrings);
      for (int i = 0; i < snippetObjects.length; i++) {
        Object snippet = snippetObjects[i];
        if (snippet != null) {
          snippetStrings[i] = snippet.toString();
        }
      }
    }

    return snippets;
  }

  /**
   * Expert: highlights the top-N passages from multiple fields, for the provided int[] docids, to
   * custom Object as returned by the {@link PassageFormatter}. Use this API to render to something
   * other than String.
   *
   * @param fieldsIn field names to highlight. Must have a stored string value.
   * @param query query to highlight.
   * @param docIdsIn containing the document IDs to highlight.
   * @param maxPassagesIn The maximum number of top-N ranked passages per-field used to form the
   *     highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets corresponding to
   *     the documents in <code>docIdsIn</code>. If no highlights were found for a document, the
   *     first {@code maxPassages} from the field will be returned.
   * @throws IOException if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without {@link
   *     IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  protected Map<String, Object[]> highlightFieldsAsObjects(
      String[] fieldsIn, Query query, int[] docIdsIn, int[] maxPassagesIn) throws IOException {
    if (fieldsIn.length < 1) {
      throw new IllegalArgumentException("fieldsIn must not be empty");
    }
    if (fieldsIn.length != maxPassagesIn.length) {
      throw new IllegalArgumentException("invalid number of maxPassagesIn");
    }
    if (searcher == null) {
      throw new IllegalStateException(
          "This method requires that an indexSearcher was passed in the "
              + "constructor.  Perhaps you mean to call highlightWithoutSearcher?");
    }

    // Sort docs & fields for sequential i/o

    // Sort doc IDs w/ index to original order: (copy input arrays since we sort in-place)
    int[] docIds = new int[docIdsIn.length];
    int[] docInIndexes = new int[docIds.length]; // fill in ascending order; points into docIdsIn[]
    copyAndSortDocIdsWithIndex(docIdsIn, docIds, docInIndexes); // latter 2 are "out" params

    // Sort fields w/ maxPassages pair: (copy input arrays since we sort in-place)
    final String[] fields = new String[fieldsIn.length];
    final int[] maxPassages = new int[maxPassagesIn.length];
    copyAndSortFieldsWithMaxPassages(
        fieldsIn, maxPassagesIn, fields, maxPassages); // latter 2 are "out" params

    // Init field highlighters (where most of the highlight logic lives, and on a per field basis)
    Set<Term> queryTerms = extractTerms(query);
    FieldHighlighter[] fieldHighlighters = new FieldHighlighter[fields.length];
    int numTermVectors = 0;
    int numPostings = 0;
    for (int f = 0; f < fields.length; f++) {
      FieldHighlighter fieldHighlighter =
          getFieldHighlighter(fields[f], query, queryTerms, maxPassages[f]);
      fieldHighlighters[f] = fieldHighlighter;

      switch (fieldHighlighter.getOffsetSource()) {
        case TERM_VECTORS:
          numTermVectors++;
          break;
        case POSTINGS:
          numPostings++;
          break;
        case POSTINGS_WITH_TERM_VECTORS:
          numTermVectors++;
          numPostings++;
          break;
        case ANALYSIS:
        case NONE_NEEDED:
        default:
          // do nothing
          break;
      }
    }

    int cacheCharsThreshold = calculateOptimalCacheCharsThreshold(numTermVectors, numPostings);

    IndexReader indexReaderWithTermVecCache =
        (numTermVectors >= 2) ? TermVectorReusingLeafReader.wrap(searcher.getIndexReader()) : null;

    // [fieldIdx][docIdInIndex] of highlightDoc result
    Object[][] highlightDocsInByField = new Object[fields.length][docIds.length];
    // Highlight in doc batches determined by loadFieldValues (consumes from docIdIter)
    DocIdSetIterator docIdIter = asDocIdSetIterator(docIds);
    for (int batchDocIdx = 0; batchDocIdx < docIds.length; ) {
      // Load the field values of the first batch of document(s) (note: commonly all docs are in
      // this batch)
      List<CharSequence[]> fieldValsByDoc = loadFieldValues(fields, docIdIter, cacheCharsThreshold);
      //    the size of the above list is the size of the batch (num of docs in the batch)

      // Highlight in per-field order first, then by doc (better I/O pattern)
      for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
        Object[] resultByDocIn = highlightDocsInByField[fieldIdx]; // parallel to docIdsIn
        FieldHighlighter fieldHighlighter = fieldHighlighters[fieldIdx];
        for (int docIdx = batchDocIdx; docIdx - batchDocIdx < fieldValsByDoc.size(); docIdx++) {
          int docId = docIds[docIdx]; // sorted order
          CharSequence content = fieldValsByDoc.get(docIdx - batchDocIdx)[fieldIdx];
          if (content == null) {
            continue;
          }
          IndexReader indexReader =
              (fieldHighlighter.getOffsetSource() == OffsetSource.TERM_VECTORS
                      && indexReaderWithTermVecCache != null)
                  ? indexReaderWithTermVecCache
                  : searcher.getIndexReader();
          final LeafReader leafReader;
          if (indexReader instanceof LeafReader) {
            leafReader = (LeafReader) indexReader;
          } else {
            List<LeafReaderContext> leaves = indexReader.leaves();
            LeafReaderContext leafReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
            leafReader = leafReaderContext.reader();
            docId -= leafReaderContext.docBase; // adjust 'doc' to be within this leaf reader
          }
          int docInIndex = docInIndexes[docIdx]; // original input order
          assert resultByDocIn[docInIndex] == null;
          resultByDocIn[docInIndex] =
              fieldHighlighter.highlightFieldForDoc(leafReader, docId, content.toString());
        }
      }

      batchDocIdx += fieldValsByDoc.size();
    }
    IOUtils.close(indexReaderWithTermVecCache); // FYI won't close underlying reader
    assert docIdIter.docID() == DocIdSetIterator.NO_MORE_DOCS
        || docIdIter.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;

    // TODO reconsider the return type; since this is an "advanced" method, lets not return a Map?
    // Notice the only
    //    caller simply iterates it to build another structure.

    // field -> object highlights parallel to docIdsIn
    Map<String, Object[]> resultMap = CollectionUtil.newHashMap(fields.length);
    for (int f = 0; f < fields.length; f++) {
      resultMap.put(fields[f], highlightDocsInByField[f]);
    }
    return resultMap;
  }

  /**
   * When cacheCharsThreshold is 0, loadFieldValues() only fetches one document at a time. We
   * override it to be 0 in two circumstances:
   */
  private int calculateOptimalCacheCharsThreshold(int numTermVectors, int numPostings) {
    if (numPostings == 0 && numTermVectors == 0) {
      // (1) When all fields are ANALYSIS there's no point in caching a batch of documents
      // because no other info on disk is needed to highlight it.
      return 0;
    } else if (numTermVectors >= 2) {
      // (2) When two or more fields have term vectors, given the field-then-doc algorithm, the
      // underlying term
      // vectors will be fetched in a terrible access pattern unless we highlight a doc at a time
      // and use a special
      // current-doc TV cache.  So we do that.  Hopefully one day TVs will be improved to make this
      // pointless.
      return 0;
    } else {
      return getCacheFieldValCharsThreshold();
    }
  }

  private void copyAndSortFieldsWithMaxPassages(
      String[] fieldsIn, int[] maxPassagesIn, final String[] fields, final int[] maxPassages) {
    System.arraycopy(fieldsIn, 0, fields, 0, fieldsIn.length);
    System.arraycopy(maxPassagesIn, 0, maxPassages, 0, maxPassagesIn.length);
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        String tmp = fields[i];
        fields[i] = fields[j];
        fields[j] = tmp;
        int tmp2 = maxPassages[i];
        maxPassages[i] = maxPassages[j];
        maxPassages[j] = tmp2;
      }

      @Override
      protected int compare(int i, int j) {
        return fields[i].compareTo(fields[j]);
      }
    }.sort(0, fields.length);
  }

  private void copyAndSortDocIdsWithIndex(
      int[] docIdsIn, final int[] docIds, final int[] docInIndexes) {
    System.arraycopy(docIdsIn, 0, docIds, 0, docIdsIn.length);
    for (int i = 0; i < docInIndexes.length; i++) {
      docInIndexes[i] = i;
    }
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int tmp = docIds[i];
        docIds[i] = docIds[j];
        docIds[j] = tmp;
        tmp = docInIndexes[i];
        docInIndexes[i] = docInIndexes[j];
        docInIndexes[j] = tmp;
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(docIds[i], docIds[j]);
      }
    }.sort(0, docIds.length);
  }

  /**
   * Highlights text passed as a parameter. This requires the {@link IndexSearcher} provided to this
   * highlighter is null. This use-case is more rare. Naturally, the mode of operation will be
   * {@link OffsetSource#ANALYSIS}. The result of this method is whatever the {@link
   * PassageFormatter} returns. For the {@link DefaultPassageFormatter} and assuming {@code content}
   * has non-zero length, the result will be a non-null string -- so it's safe to call {@link
   * Object#toString()} on it in that case.
   *
   * @param field field name to highlight (as found in the query).
   * @param query query to highlight.
   * @param content text to highlight.
   * @param maxPassages The maximum number of top-N ranked passages used to form the highlighted
   *     snippets.
   * @return result of the {@link PassageFormatter} -- probably a String. Might be null.
   * @throws IOException if an I/O error occurred during processing
   */
  // TODO make content a List? and return a List? and ensure getEmptyHighlight is never invoked
  // multiple times?
  public Object highlightWithoutSearcher(String field, Query query, String content, int maxPassages)
      throws IOException {
    if (this.searcher != null) {
      throw new IllegalStateException(
          "highlightWithoutSearcher should only be called on a "
              + getClass().getSimpleName()
              + " without an IndexSearcher.");
    }
    Objects.requireNonNull(content, "content is required");
    Set<Term> queryTerms = extractTerms(query);
    return getFieldHighlighter(field, query, queryTerms, maxPassages)
        .highlightFieldForDoc(null, -1, content);
  }

  protected FieldHighlighter getFieldHighlighter(
      String field, Query query, Set<Term> allTerms, int maxPassages) {
    Set<String> maskedFields = getMaskedFields(field);
    FieldOffsetStrategy fieldOffsetStrategy;
    if (maskedFields == null || maskedFields.isEmpty()) {
      UHComponents components = getHighlightComponents(field, query, allTerms);
      OffsetSource offsetSource = getOptimizedOffsetSource(components);
      fieldOffsetStrategy = getOffsetStrategy(offsetSource, components);
    } else {
      List<FieldOffsetStrategy> fieldsOffsetStrategies = new ArrayList<>(maskedFields.size() + 1);
      for (String maskedField : maskedFields) {
        UHComponents components = getHighlightComponents(maskedField, query, allTerms);
        OffsetSource offsetSource = getOptimizedOffsetSource(components);
        fieldsOffsetStrategies.add(getOffsetStrategy(offsetSource, components));
      }
      // adding original field as well
      UHComponents components = getHighlightComponents(field, query, allTerms);
      OffsetSource offsetSource = getOptimizedOffsetSource(components);
      fieldsOffsetStrategies.add(getOffsetStrategy(offsetSource, components));

      fieldOffsetStrategy = new MultiFieldsOffsetStrategy(fieldsOffsetStrategies);
    }
    return newFieldHighlighter(
        field,
        fieldOffsetStrategy,
        new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR),
        getScorer(field),
        maxPassages,
        getMaxNoHighlightPassages(field),
        getFormatter(field),
        getPassageSortComparator(field));
  }

  protected FieldHighlighter newFieldHighlighter(
      String field,
      FieldOffsetStrategy fieldOffsetStrategy,
      BreakIterator breakIterator,
      PassageScorer passageScorer,
      int maxPassages,
      int maxNoHighlightPassages,
      PassageFormatter passageFormatter,
      Comparator<Passage> passageSortComparator) {
    return new FieldHighlighter(
        field,
        fieldOffsetStrategy,
        breakIterator,
        passageScorer,
        maxPassages,
        maxNoHighlightPassages,
        passageFormatter,
        passageSortComparator);
  }

  protected UHComponents getHighlightComponents(String field, Query query, Set<Term> allTerms) {
    Predicate<String> fieldMatcher = getFieldMatcher(field);
    Set<HighlightFlag> highlightFlags = getFlags(field);
    PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
    boolean queryHasUnrecognizedPart = hasUnrecognizedQuery(fieldMatcher, query);
    BytesRef[] terms = null;
    LabelledCharArrayMatcher[] automata = null;
    if (!highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES) || !queryHasUnrecognizedPart) {
      terms = filterExtractedTerms(fieldMatcher, allTerms);
      automata = getAutomata(field, query, highlightFlags);
    } // otherwise don't need to extract
    return new UHComponents(
        field,
        fieldMatcher,
        query,
        terms,
        phraseHelper,
        automata,
        queryHasUnrecognizedPart,
        highlightFlags);
  }

  protected boolean hasUnrecognizedQuery(Predicate<String> fieldMatcher, Query query) {
    boolean[] hasUnknownLeaf = new boolean[1];
    query.visit(
        new QueryVisitor() {
          @Override
          public boolean acceptField(String field) {
            // checking hasUnknownLeaf is a trick to exit early
            return hasUnknownLeaf[0] == false && fieldMatcher.test(field);
          }

          @Override
          public void visitLeaf(Query query) {
            if (MultiTermHighlighting.canExtractAutomataFromLeafQuery(query) == false) {
              if (!(query instanceof MatchAllDocsQuery || query instanceof MatchNoDocsQuery)) {
                hasUnknownLeaf[0] = true;
              }
            }
          }
        });
    return hasUnknownLeaf[0];
  }

  protected static BytesRef[] filterExtractedTerms(
      Predicate<String> fieldMatcher, Set<Term> queryTerms) {
    // Strip off the redundant field and sort the remaining terms
    SortedSet<BytesRef> filteredTerms = new TreeSet<>();
    for (Term term : queryTerms) {
      if (fieldMatcher.test(term.field())) {
        filteredTerms.add(term.bytes());
      }
    }
    return filteredTerms.toArray(new BytesRef[filteredTerms.size()]);
  }

  protected PhraseHelper getPhraseHelper(
      String field, Query query, Set<HighlightFlag> highlightFlags) {
    boolean useWeightMatchesIter = highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES);
    if (useWeightMatchesIter) {
      return PhraseHelper.NONE; // will be handled by Weight.matches which always considers phrases
    }
    boolean highlightPhrasesStrictly = highlightFlags.contains(HighlightFlag.PHRASES);
    boolean handleMultiTermQuery = highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY);
    return highlightPhrasesStrictly
        ? new PhraseHelper(
            query,
            field,
            getFieldMatcher(field),
            this::requiresRewrite,
            this::preSpanQueryRewrite,
            !handleMultiTermQuery)
        : PhraseHelper.NONE;
  }

  protected LabelledCharArrayMatcher[] getAutomata(
      String field, Query query, Set<HighlightFlag> highlightFlags) {
    // do we "eagerly" look in span queries for automata here, or do we not and let PhraseHelper
    // handle those?
    // if don't highlight phrases strictly,
    final boolean lookInSpan =
        !highlightFlags.contains(HighlightFlag.PHRASES) // no PhraseHelper
            || highlightFlags.contains(
                HighlightFlag.WEIGHT_MATCHES); // Weight.Matches will find all

    return highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY)
        ? MultiTermHighlighting.extractAutomata(query, getFieldMatcher(field), lookInSpan)
        : ZERO_LEN_AUTOMATA_ARRAY;
  }

  protected OffsetSource getOptimizedOffsetSource(UHComponents components) {
    OffsetSource offsetSource = getOffsetSource(components.getField());

    // null automata means unknown, so assume a possibility
    boolean mtqOrRewrite =
        components.getAutomata() == null
            || components.getAutomata().length > 0
            || components.getPhraseHelper().willRewrite()
            || components.hasUnrecognizedQueryPart();

    // null terms means unknown, so assume something to highlight
    if (mtqOrRewrite == false
        && components.getTerms() != null
        && components.getTerms().length == 0) {
      return OffsetSource.NONE_NEEDED; // nothing to highlight
    }

    switch (offsetSource) {
      case POSTINGS:
        if (mtqOrRewrite) { // may need to see scan through all terms for the highlighted document
          // efficiently
          return OffsetSource.ANALYSIS;
        }
        break;
      case POSTINGS_WITH_TERM_VECTORS:
        if (mtqOrRewrite == false) {
          return OffsetSource.POSTINGS; // We don't need term vectors
        }
        break;
      case ANALYSIS:
      case TERM_VECTORS:
      case NONE_NEEDED:
      default:
        // stick with the original offset source
        break;
    }

    return offsetSource;
  }

  protected FieldOffsetStrategy getOffsetStrategy(
      OffsetSource offsetSource, UHComponents components) {
    switch (offsetSource) {
      case ANALYSIS:
        if (!components.getPhraseHelper().hasPositionSensitivity()
            && !components.getHighlightFlags().contains(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED)
            && !components.getHighlightFlags().contains(HighlightFlag.WEIGHT_MATCHES)) {
          // skip using a memory index since it's pure term filtering
          return new TokenStreamOffsetStrategy(components, getIndexAnalyzer());
        } else {
          return new MemoryIndexOffsetStrategy(components, getIndexAnalyzer());
        }
      case NONE_NEEDED:
        return NoOpOffsetStrategy.INSTANCE;
      case TERM_VECTORS:
        return new TermVectorOffsetStrategy(components);
      case POSTINGS:
        return new PostingsOffsetStrategy(components);
      case POSTINGS_WITH_TERM_VECTORS:
        return new PostingsWithTermVectorsOffsetStrategy(components);
      default:
        throw new IllegalArgumentException("Unrecognized offset source " + offsetSource);
    }
  }

  /**
   * When highlighting phrases accurately, we need to know which {@link SpanQuery}'s need to have
   * {@link Query#rewrite(IndexSearcher)} called on them. It helps performance to avoid it if it's
   * not needed. This method will be invoked on all SpanQuery instances recursively. If you have
   * custom SpanQuery queries then override this to check instanceof and provide a definitive
   * answer. If the query isn't your custom one, simply return null to have the default rules apply,
   * which govern the ones included in Lucene.
   */
  protected Boolean requiresRewrite(SpanQuery spanQuery) {
    return null;
  }

  /**
   * When highlighting phrases accurately, we may need to handle custom queries that aren't
   * supported in the {@link org.apache.lucene.search.highlight.WeightedSpanTermExtractor} as called
   * by the {@code PhraseHelper}. Should custom query types be needed, this method should be
   * overriden to return a collection of queries if appropriate, or null if nothing to do. If the
   * query is not custom, simply returning null will allow the default rules to apply.
   *
   * @param query Query to be highlighted
   * @return A Collection of Query object(s) if needs to be rewritten, otherwise null.
   */
  protected Collection<Query> preSpanQueryRewrite(Query query) {
    return null;
  }

  private DocIdSetIterator asDocIdSetIterator(int[] sortedDocIds) {
    return new DocIdSetIterator() {
      int idx = -1;

      @Override
      public int docID() {
        if (idx < 0 || idx >= sortedDocIds.length) {
          return NO_MORE_DOCS;
        }
        return sortedDocIds[idx];
      }

      @Override
      public int nextDoc() throws IOException {
        idx++;
        return docID();
      }

      @Override
      public int advance(int target) throws IOException {
        return super.slowAdvance(target); // won't be called, so whatever
      }

      @Override
      public long cost() {
        return Math.max(0, sortedDocIds.length - (idx + 1)); // remaining docs
      }
    };
  }

  /**
   * Loads the String values for each docId by field to be highlighted. By default this loads from
   * stored fields by the same name as given, but a subclass can change the source. The returned
   * Strings must be identical to what was indexed (at least for postings or term-vectors offset
   * sources). This method must load fields for at least one document from the given {@link
   * DocIdSetIterator} but need not return all of them; by default the character lengths are summed
   * and this method will return early when {@code cacheCharsThreshold} is exceeded. Specifically if
   * that number is 0, then only one document is fetched no matter what. Values in the array of
   * {@link CharSequence} will be null if no value was found.
   */
  protected List<CharSequence[]> loadFieldValues(
      String[] fields, DocIdSetIterator docIter, int cacheCharsThreshold) throws IOException {
    List<CharSequence[]> docListOfFields =
        new ArrayList<>(cacheCharsThreshold == 0 ? 1 : (int) Math.min(64, docIter.cost()));

    LimitedStoredFieldVisitor visitor = newLimitedStoredFieldsVisitor(fields);
    StoredFields storedFields = searcher.storedFields();
    int sumChars = 0;
    do {
      int docId = docIter.nextDoc();
      if (docId == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      visitor.init();
      storedFields.document(docId, visitor);
      CharSequence[] valuesByField = visitor.getValuesByField();
      docListOfFields.add(valuesByField);
      for (CharSequence val : valuesByField) {
        sumChars += (val == null ? 0 : val.length());
      }
    } while (sumChars <= cacheCharsThreshold && cacheCharsThreshold != 0);
    return docListOfFields;
  }

  /**
   * @lucene.internal
   */
  protected LimitedStoredFieldVisitor newLimitedStoredFieldsVisitor(String[] fields) {
    return new LimitedStoredFieldVisitor(fields, MULTIVAL_SEP_CHAR, getMaxLength());
  }

  /**
   * Fetches stored fields for highlighting. Uses a multi-val separator char and honors a max length
   * to retrieve.
   *
   * @lucene.internal
   */
  protected static class LimitedStoredFieldVisitor extends StoredFieldVisitor {
    protected final String[] fields;
    protected final char valueSeparator;
    protected final int maxLength;
    protected CharSequence[] values; // starts off as String; may become StringBuilder.
    protected int currentField;

    public LimitedStoredFieldVisitor(String[] fields, char valueSeparator, int maxLength) {
      this.fields = fields;
      this.valueSeparator = valueSeparator;
      this.maxLength = maxLength;
    }

    void init() {
      values = new CharSequence[fields.length];
      currentField = -1;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      assert currentField >= 0;
      Objects.requireNonNull(value, "String value should not be null");
      CharSequence curValue = values[currentField];
      if (curValue == null) {
        // question: if truncate due to maxLength, should we try and avoid keeping the other chars
        // in-memory on
        //  the backing char[]?
        values[currentField] =
            value.substring(0, Math.min(maxLength, value.length())); // note: may return 'this'
        return;
      }
      final int lengthBudget = maxLength - curValue.length();
      if (lengthBudget <= 0) {
        return;
      }
      StringBuilder curValueBuilder;
      if (curValue instanceof StringBuilder) {
        curValueBuilder = (StringBuilder) curValue;
      } else {
        // upgrade String to StringBuilder. Choose a good initial size.
        curValueBuilder =
            new StringBuilder(curValue.length() + Math.min(lengthBudget, value.length() + 256));
        curValueBuilder.append(curValue);
      }
      curValueBuilder.append(valueSeparator);
      curValueBuilder.append(value, 0, Math.min(lengthBudget - 1, value.length()));
      values[currentField] = curValueBuilder;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      currentField = Arrays.binarySearch(fields, fieldInfo.name);
      if (currentField < 0) {
        return Status.NO;
      }
      CharSequence curVal = values[currentField];
      if (curVal != null && curVal.length() >= maxLength) {
        return fields.length == 1 ? Status.STOP : Status.NO;
      }
      return Status.YES;
    }

    CharSequence[] getValuesByField() {
      return this.values;
    }
  }

  /**
   * Wraps an IndexReader that remembers/caches the last call to {@link TermVectors#get(int)} so
   * that if the next call has the same ID, then it is reused. If TV's were column-stride (like
   * doc-values), there would be no need for this.
   */
  private static class TermVectorReusingLeafReader extends FilterLeafReader {

    static IndexReader wrap(IndexReader reader) throws IOException {
      LeafReader[] leafReaders =
          reader.leaves().stream()
              .map(LeafReaderContext::reader)
              .map(TermVectorReusingLeafReader::new)
              .toArray(LeafReader[]::new);
      return new BaseCompositeReader<IndexReader>(leafReaders, null) {
        @Override
        protected void doClose() { // don't close the underlying reader
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null;
        }
      };
    }

    private int lastDocId = -1;
    private Fields tvFields;

    TermVectorReusingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
      if (docID != lastDocId) {
        lastDocId = docID;
        tvFields = in.getTermVectors(docID);
      }
      return tvFields;
    }

    @Override
    public TermVectors termVectors() throws IOException {
      TermVectors orig = in.termVectors();
      return new TermVectors() {
        @Override
        public Fields get(int docID) throws IOException {
          if (docID != lastDocId) {
            lastDocId = docID;
            tvFields = orig.get(docID);
          }
          return tvFields;
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

  /** Flags for controlling highlighting behavior. */
  public enum HighlightFlag {
    /**
     * @see Builder#withHighlightPhrasesStrictly(boolean)
     */
    PHRASES,

    /**
     * @see Builder#withHandleMultiTermQuery(boolean)
     */
    MULTI_TERM_QUERY,

    /**
     * @see Builder#withPassageRelevancyOverSpeed(boolean)
     */
    PASSAGE_RELEVANCY_OVER_SPEED,

    /**
     * @see Builder#withWeightMatches(boolean)
     */
    WEIGHT_MATCHES

    // TODO: useQueryBoosts
  }
}
