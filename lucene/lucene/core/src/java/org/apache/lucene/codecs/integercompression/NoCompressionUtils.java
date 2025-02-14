package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class NoCompressionUtils {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public static void encode(long[] longs, DataOutput out) throws IOException
    {
        for (int i = 0; i < longs.length; i++) {
            out.writeLong(longs[i]);
        }
    }

    public static void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeInt(input);
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public static void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        pdu.in.readLongs(longs, 0, 128);
    }
}
