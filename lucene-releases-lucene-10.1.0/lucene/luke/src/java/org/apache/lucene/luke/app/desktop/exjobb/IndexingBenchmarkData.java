package org.apache.lucene.luke.app.desktop.exjobb;


import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.util.compress.TermCompressionMode;

public class IndexingBenchmarkData {
    public IntegerCompressionType integerCompressionType;
    public TermCompressionMode termCompressionMode;
    public long totalIndexSizeInMB;
    public long totalIndexingTimeInMS;
}
