package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Implements FOR compression for integer sequences.
 */
public class FORCompression implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        // We store the reference as a VInt
        long reference = positions[0];
        out.writeLong(reference);

        // Now store the offsets from the reference
        for (int i = 1; i < 128; i++) {
            long offset = positions[i] - reference;
            out.writeLong(offset);
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
        longs[0] = pdu.in.readLong();
        for (int i = 1; i < 128; i++) {
            longs[i] = pdu.in.readLong() + longs[0];
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
