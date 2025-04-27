package org.apache.lucene.codecs.exjobb.integercompression;


import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.codecs.lucene101.ForUtil;
import org.apache.lucene.codecs.lucene101.PForUtil;
import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;
import jdk.incubator.vector.*;
import static jdk.incubator.vector.VectorOperators.*;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Implements FOR compression for integer sequences.
 */
public final class LimitTestCompressor implements IntegerCompressor {

    ForUtil forUtil = new ForUtil();
    // https://en.wikipedia.org/wiki/Delta_encoding
    /** FOR Encode 128 integers from {@code longs} into {@code out}. */
    public static long averageShortTime = 0;
    private long averageShortCount = 0;
    public static long averageIntTime = 0;
    private long averageIntCount = 0;
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
        if (bitWidth == 16)
            bitWidthMultipleOf8 = 32;
        if (bitWidthMultipleOf8 == 24)
            bitWidthMultipleOf8 = 32;
        if (bitWidthMultipleOf8 == 8)
            bitWidthMultipleOf8 = 32;
        if (bitWidthMultipleOf8 == 0)
            bitWidthMultipleOf8 = 32;
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
    /**
     * Delta Decode 128 integers into {@code ints}.
     *
     * @return
     */
    public boolean decode(PostingDecodingUtil pdu, int[] ints, HashMap<Integer, ArrayList<Integer>> exceptions, short[] shorts) throws IOException {
        final byte token = pdu.in.readByte();
        final int bitWidth = (byte) (token & 0b01111111);
        final int isAllEqual = (byte) (token & 0b10000000);
        var in = pdu.in;
//        int minValue = pdu.in.readVInt();

        if (isAllEqual == 0) {
            switch (bitWidth) {
                case 8:  // 1 byte

                    pdu.in.read1ByteToInts(ints, 0, 128);
                    //in.readInts(ints, 0, 32);
                    break;

                case 16: // 2 bytes
                    long start = System.nanoTime();
               //     if (shorts == null)
                     //   pdu.in.read2ByteToInts(ints, 0, 128);

                        //pdu.in.readShorts(shorts, 0, 128);
                    pdu.in.readInts(ints, 0, 64);

                    IntegerCompressionUtils.intToShort(ints, shorts);
                    long duration = System.nanoTime() - start;
                       // for (int i = 0; i < 128; i++) {

                       //     ints[i] = shorts[i] & 0xFFFF;
                       // }

                    averageShortCount += 1;
                    averageShortTime += (duration - averageShortCount) / averageShortCount;
                    return true;
                   // else {
                    //  in.readShorts(shorts, 0, 128);

                  //    return true;
                   // }


               //     for (int i = 0; i < 128; i++) {
                //        ints[i] = shorts[i];
                //    }
                //    break;
                  //  pdu.in.readInts(ints, 0, 64);
                  //  break;


                case 24: // 3 bytes
                    pdu.in.read3ByteToInts(ints, 0, 128);
                   // in.readInts(ints, 0, 96);
                    break;

                case 32: // 4 bytes
                    long start2 = System.nanoTime();
                    in.readInts(ints, 0, 128);
                    long duration2 = System.nanoTime() - start2;
                    averageIntCount += 1;
                    averageIntTime += (duration2 - averageIntCount) / averageIntCount;

                    break;

                default:
                    throw new IOException("Unsupported bit width: " + bitWidth);
            }

        }
        else {
            Arrays.fill(ints, 0, ForUtil.BLOCK_SIZE, in.readVInt());
        }
        return false;
    }




    @Override
    public void skip(IndexInput in) throws IOException {
        final int token = in.readByte();
        final int bitWidth = (byte) (token & 0b01111111);
        final int isAllEqual = (byte) (token & 0b10000000);
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
