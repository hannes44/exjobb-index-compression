package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.internal.vectorization.PostingDecodingUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class NoCompressionUtils {

    // https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Encode 128 integers from {@code longs} into {@code out}. */
    public static void encode(long[] longs, DataOutput out) throws IOException
    {

        //out.writeByte(bytesRequiredPerValue);

        // Write the first value as-is
      //  out.writeLong(longs[0]);

        for (int i = 0; i < longs.length; i++) {
            out.writeLong(longs[i]);
     //       out.writeBytes(longToBytes(longs[i], bytesRequiredPerValue), 0, bytesRequiredPerValue);
        }

    }

    //https://en.wikipedia.org/wiki/Delta_encoding
    /** Delta Decode 128 integers into {@code ints}. */
    public static void decode(PostingDecodingUtil pdu, long[] longs) throws IOException {
        pdu.in.readLongs(longs, 0, 128);


      //  for (int i = 0; i < longs.length; i++) {
      //      longs[i] =  pdu.in.readLong();
      //  }
    }


}
