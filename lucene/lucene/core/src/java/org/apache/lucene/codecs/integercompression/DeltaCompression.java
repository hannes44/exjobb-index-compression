package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class DeltaCompression {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public static void encode(long[] deltas, DataOutput out) throws IOException
    {
        for (int i = 0; i < deltas.length; i++) {
            out.writeVInt((int)(deltas[i]));
        }
    }

    public static void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeInt(input);
    }

    public static int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readInt();
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public static void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        longs[0] = pdu.in.readVInt();
        for (int i = 1; i < longs.length; i++) {
            longs[i] = pdu.in.readVInt(); //+ longs[i-1];
        }
    }
}
