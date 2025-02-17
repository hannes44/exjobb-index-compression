package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class DeltaCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        out.writeVInt((int)(positions[0]));
        for (int i = 1; i < positions.length; i++) {
            int delta = (int)(positions[i] - positions[i-1]);
            out.writeVInt(delta);
        }
    }

    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeVInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readVInt();
    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        longs[0] = pdu.in.readVInt();
        for (int i = 1; i < longs.length; i++) {
            longs[i] = pdu.in.readVInt() + longs[i-1];
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.DELTA;
    }
}
