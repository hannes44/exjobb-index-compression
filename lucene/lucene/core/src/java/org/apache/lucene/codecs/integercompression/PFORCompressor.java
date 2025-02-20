package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

// Frame of Reference compression
public class PFORCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        // We store the reference as a VInt
        int reference = (int) positions[0];
        out.writeVInt(reference);

        // Now store the offsets from the reference
        for (int i = 1; i < positions.length; i++) {
            int offset = (int) positions[i] - reference;
            out.writeVInt(offset);
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
            longs[i] = pdu.in.readVInt() + longs[0];
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
