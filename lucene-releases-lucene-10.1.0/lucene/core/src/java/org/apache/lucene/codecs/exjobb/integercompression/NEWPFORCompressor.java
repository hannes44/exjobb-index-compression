package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.*;

/**
 * Implements FOR compression for integer sequences.
 */
public class NEWPFORCompressor implements IntegerCompressor {

    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    // TODO: try using normal bitpacking instead of variable integers
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {
        // We store the reference as a VInt
        int maxValue = IntegerCompressionUtils.getMaxValue(ints);

        HashMap<Integer, List<Integer>> bitCountToIndex = IntegerCompressionUtils.getBitCountToIndexMap(ints);

        int maxBitsRequired = PackedInts.bitsRequired(maxValue);

        IntegerCompressionUtils.CostFunction costFunction = (bitWidth, totalExceptions, maxBitWidth) -> (bitWidth * (128 - totalExceptions) + totalExceptions * 16);
        int bestBitWidth = IntegerCompressionUtils.getBestBitWidth(costFunction, maxBitsRequired, bitCountToIndex);

        List<Integer> exceptionIndices = new ArrayList<>();
        List<Integer> exceptionValues = new ArrayList<>();
        IntegerCompressionUtils.getExceptionsFromIndexMap(exceptionIndices, exceptionValues, bitCountToIndex, ints, bestBitWidth);

        byte exceptionCount = (byte)exceptionIndices.size();

        out.writeByte((byte)bestBitWidth);
        out.writeByte(exceptionCount);

        ForUtil forUtil = new ForUtil();
        final long maxUnpatchedValue = (1L << bestBitWidth) - 1;
        for (int i = 0; i < 128; i++) {
            ints[i] &= maxUnpatchedValue;
        }

        forUtil.encode(ints, bestBitWidth, out);

        int count = 0;
        // Now the exceptions Lists
        for (int index : exceptionIndices)
        {
            out.writeByte((byte)index);
            out.writeVInt(IntegerCompressionUtils.getLeftBits(exceptionValues.get(count), 32 - bestBitWidth));
            count++;
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
    /**
     * Delta Decode 128 integers into {@code ints}.
     *
     * @return
     */
    public boolean decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions, short[] shorts) throws IOException {
        int regularValueBitWidth = Byte.toUnsignedInt(pdu.in.readByte());

        byte exceptionCount = pdu.in.readByte();
        byte[] exceptionBitMask = new byte[16];

        ForUtil forUtil = new ForUtil();

        forUtil.decode(regularValueBitWidth, pdu, ints);

        for (int i = 0; i < exceptionCount; i++) {
            byte index = pdu.in.readByte();
            int value = pdu.in.readVInt();
            value = value << regularValueBitWidth;
            ints[index] += value;
        }

        return false;
    }

    @Override
    public void skip(IndexInput in) throws IOException {
        int regularValueBitWidth = Byte.toUnsignedInt(in.readByte());
        byte exceptionCount = in.readByte();

        in.skipBytes(ForUtil.numBytes(regularValueBitWidth));

        for (int i = 0; i < exceptionCount; i++) {
            in.readByte();
            in.readVInt();
        }
    }

    public IntegerCompressionType getType() {
        return IntegerCompressionType.NEWPFOR;
    }
}
