package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Implements FOR compression for integer sequences.
 */
public class NEWPFORCompressor implements IntegerCompressor {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] positions, DataOutput out) throws IOException
    {
        //IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);

        // We store the reference as a VInt
        int minValue = IntegerCompressionUtils.getMinValue(positions);
        int maxValue = IntegerCompressionUtils.getMaxValue(positions);

        int maxBitsRequired = PackedInts.bitsRequired(maxValue - minValue);

        out.writeVInt(minValue);
        out.writeVInt(maxBitsRequired);

        ForUtil forUtil = new ForUtil();

        // Now store the offsets from the reference
        for (int i = 0; i < 128; i++) {
            positions[i] = positions[i] - minValue;
        }
        forUtil.encode(positions, maxBitsRequired, out);
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
    public void decode(PostingDecodingUtil pdu, int[] ints) throws IOException {
        int minValue = pdu.in.readVInt();
        int maxBits = pdu.in.readVInt();
        ForUtil forUtil = new ForUtil();

        forUtil.decode(maxBits, pdu, ints);

        for (int i = 0; i < 128; i++) {
            ints[i] += minValue;
        }

        //IntegerCompressionUtils.turnAbsolutesIntoDeltas(ints);
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
