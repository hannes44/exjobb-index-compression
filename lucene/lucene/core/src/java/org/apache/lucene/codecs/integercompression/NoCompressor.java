package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Implements no compression for integer sequences.
 */
public class NoCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(long[] longs, DataOutput out) throws IOException
    {
        for (int i = 0; i < longs.length; i++) {
            out.writeLong(longs[i]);
        }
    }

    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readInt();
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        pdu.in.readLongs(longs, 0, 128);
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.NONE;
    }
}
