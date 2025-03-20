package org.apache.lucene.codecs.exjobb.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Factory class for creating IntegerCompressor objects.
 */
public class IntegerCompressionFactory {
    public static IntegerCompressor CreateIntegerCompressor(IntegerCompressionType compressionType)
    {
        switch (compressionType) {
            case DELTA:
                return new DeltaCompressor();
            case NONE:
                return new NoCompressor();
            case FOR:
                return new FORCompression();
            case PFOR:
                return new PFORCompressor();
            case DEFAULT:
                return new LuceneDefaultCompressor();
            case LIMITTTEST:
                return new LimitTestCompressor();
            case FASTPFOR:
                return new FASTPFORCompressor();
            case LIMITTEST2:
                return new LimitTest2Compressor();
            case SIMPLE8B:
                return new Simple8bCompressor();
            case NEWPFOR:
                return new NEWPFORCompressor();
            default:
                System.out.println("ERROR: compression type not supported! Using Delta Compression");
                System.out.println(compressionType);
                return new LuceneDefaultCompressor();
        }

    }
}
