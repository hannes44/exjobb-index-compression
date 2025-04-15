package org.apache.lucene.codecs.exjobb.integercompression;


import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.codecs.lucene101.PForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Implements FOR compression for integer sequences.
 */
public final class LimitTestCompressor implements IntegerCompressor {
    ForUtil forUtil = new ForUtil();
    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    public void encode(int[] ints, DataOutput out, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException
    {

        //IntegerCompressionUtils.turnDeltasIntoAbsolutes(positions);
        // We store the reference as a VInt
        int minValue = ints[0];
        int maxValue = ints[0];
        for (int i = 0; i < 128; i++) {
            if (minValue > ints[i])
                minValue = ints[i];
            if (maxValue < ints[i])
                maxValue = ints[i];
        }
  //      for (int i = 0; i < 128; i++) {
            //          ints[i] -= minValue;
 //       }

        int bitWidth = PackedInts.bitsRequired(maxValue);
        int bitWidthMultipleOf8 = ((bitWidth + 7) / 8) * 8;
        // To most significant bit is flag for if all vales are equal, the rest 7 bits are the bitwidth
        byte token = (byte)bitWidthMultipleOf8;

        // out.writeVInt(minValue);

        if (PForUtil.allEqual(ints)) {
            token = (byte) (token | 0x80);
            out.writeByte(token);
            out.writeVInt(ints[0]);
        } else {
            out.writeByte(token);
            int numBytes = bitWidthMultipleOf8 / 8;
            for (int i = 0; i < 128; i++) {
                // Write bytes from least significant to most significant
                for (int j = 0; j < numBytes; j++) {
                    out.writeByte((byte)((ints[i] >> (8 * j)) & 0xFF));
                }
            }

        }
    }




    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public void decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions) throws IOException {
        byte token = pdu.in.readByte();
        int bitWidth = (byte) (token & 0b01111111);
        int isAllEqual = (byte) (token & 0b10000000);
//        int minValue = pdu.in.readVInt();

        if (isAllEqual == 0) {
            switch (bitWidth) {
                case 8:  // 1 byte

                    pdu.in.read1ByteToInts(ints, 0, 128);
                    break;

                case 16: // 2 bytes

                    pdu.in.read2ByteToInts(ints, 0, 128);
                    break;

                case 24: // 3 bytes
                    pdu.in.read3ByteToInts(ints, 0, 128);
                    break;

                case 32: // 4 bytes
                    pdu.in.readInts(ints, 0, 128);

                    break;

                default:
                    throw new IOException("Unsupported bit width: " + bitWidth);
            }

        }
        else {
            Arrays.fill(ints, 0, ForUtil.BLOCK_SIZE, pdu.in.readVInt());
        }
    }



    @Override
    public void skip(IndexInput in) throws IOException {
        int token = in.readByte();
        int bitWidth = (byte) (token & 0b01111111);
        int isAllEqual = (byte) (token & 0b10000000);
        //     int minValue = in.readVInt();
        if (isAllEqual == 0) {
            in.skipBytes(128 * bitWidth / 8);
        }
        else {
            in.readVInt();
        }

    }


    public void encodeSingleInt(int input, DataOutput out) throws IOException {
        out.writeVInt(input);
    }

    public int decodeSingleInt(IndexInput input) throws IOException
    {
        return input.readVInt();
    }


    public IntegerCompressionType getType() {
        return IntegerCompressionType.FOR;
    }
}
