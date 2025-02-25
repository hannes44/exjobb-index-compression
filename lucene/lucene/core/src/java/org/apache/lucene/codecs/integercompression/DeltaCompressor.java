package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Implements delta compression for integer sequences.
 */
public class DeltaCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        // This has to be long, otherwise it is possible to get overflow for large index
        out.writeLong((int)positions[0]);

        for (int i = 1; i < positions.length; i++) {
            long delta = (positions[i] - positions[i-1]);
            out.writeLong(delta);
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

        longs[0] = pdu.in.readLong();
        for (int i = 1; i < 128; i++) {
            longs[i] = pdu.in.readLong() + longs[i-1];
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.DELTA;
    }
}
