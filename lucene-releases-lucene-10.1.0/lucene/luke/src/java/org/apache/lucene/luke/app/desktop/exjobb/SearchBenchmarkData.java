package org.apache.lucene.luke.app.desktop.exjobb;

import org.apache.lucene.codecs.exjobb.integercompression.IntegerCompressionType;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;

public class SearchBenchmarkData {
    public IntegerCompressionType integerCompressionType;
    public Lucene90BlockTreeTermsWriter.TermCompressionMode termCompressionMode;
    public double averageQuerySearchTimeInMS;
}
