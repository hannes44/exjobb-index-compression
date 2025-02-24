package org.apache.lucene.codecs.integercompression;

import org.apache.lucene.store.DataOutput;

public class Bitpacker {

    

    static public void packLongs(long[] longs, int bitsPerValue, DataOutput out) {
        int numberOfLongs = longs.length;
        int totalBitCount = numberOfLongs * bitsPerValue;

        // The smallest size we can store is byte


    }
}
