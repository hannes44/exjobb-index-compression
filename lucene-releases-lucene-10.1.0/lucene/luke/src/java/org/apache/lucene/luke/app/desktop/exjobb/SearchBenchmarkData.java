package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.util.compress.TermCompressionMode;

public class SearchBenchmarkData {
    public IntegerCompressionType integerCompressionType;
    public TermCompressionMode termCompressionMode;
    public double averageQuerySearchTimeInMS;
}
