package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class DeltaCompression
{
    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public static void encode(long[] longs, DataOutput out) throws IOException
    {
        long maxDelta = 0;

        // Encode the rest of the values as deltas
        for (int i = 1; i < longs.length; i++) {
            long delta = longs[i] - longs[i - 1]; // Calculate the delta
            if (delta > maxDelta)
                maxDelta = delta;
        }

        long bitsPerValue = PackedInts.bitsRequired(maxDelta);



        byte bytesRequiredPerValue = (byte)(bitsPerValue/8);

        out.writeByte(bytesRequiredPerValue);

        // Write the first value as-is
        out.writeLong(longs[0]);

        for (int i = 1; i < longs.length; i++) {
            out.writeBytes(longToBytes(longs[i], bytesRequiredPerValue), 0, bytesRequiredPerValue);
        }

    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public static void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        pdu.in.readLongs(longs, 0, 128);
        byte bytesPerValue = pdu.in.readByte();

        // First long

        longs[0] = pdu.in.readLong();

        for (int i = 1; i < longs.length; i++) {
            byte[] bytes = new byte[bytesPerValue];
            pdu.in.readBytes(bytes, 0, bytesPerValue);
            longs[i] = 0;

            for (int j = 0; j < bytesPerValue; j++) {
                longs[i] = (longs[i] << 8) | (bytes[j] & 0xFF);
            }
        }
    }

    public static byte[] longToBytes(long l, int numberOfBytes) {
        byte[] result = new byte[numberOfBytes];
        for (int i = numberOfBytes-1; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return result;
    }

}