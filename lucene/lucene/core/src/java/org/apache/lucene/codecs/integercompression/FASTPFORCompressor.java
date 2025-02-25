package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.codecs.lucene912.ForUtil;
import org.apache.lucene.codecs.lucene912.IntegerCompressionType;
import org.apache.lucene.codecs.lucene912.IntegerCompressor;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Implements FOR compression for integer sequences.
 */
public class FASTPFORCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(long[] positions, DataOutput out) throws IOException
    {
        // We store the reference as a VInt
        long minValue = positions[0];
        long maxValue = positions[0];
        for (int i = 0; i < 128; i++) {
            if (positions[i] > maxValue)
            {
                maxValue = positions[i];
            }
            if (positions[i] < minValue)
            {
                minValue = positions[i];
            }
        }

        long maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);

        out.writeVLong(minValue);
        out.writeVLong(maxBitsRequired);

        ForUtil forUtil = new ForUtil();

        // Now store the offsets from the reference
        for (int i = 0; i < 128; i++) {
            positions[i] = positions[i] - minValue;
        }
        forUtil.encode(positions, (int) maxBitsRequired, out);
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
        long minValue = pdu.in.readVLong();
        long maxBits = pdu.in.readVLong();
        ForUtil forUtil = new ForUtil();

        forUtil.decode((int)maxBits, pdu, longs);

        for (int i = 0; i < 128; i++) {
            longs[i] += minValue;
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
