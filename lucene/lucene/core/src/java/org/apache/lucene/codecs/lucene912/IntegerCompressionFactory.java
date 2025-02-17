package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.codecs.integercompression.DeltaCompressor;
import org.apache.lucene.codecs.integercompression.FORCompression;
import org.apache.lucene.codecs.integercompression.NoCompressor;
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
            default:
                System.out.println("ERROR: compression type not supported! Using Delta Compression");
                System.out.println(compressionType);
                return new DeltaCompressor();
        }

    }
}
