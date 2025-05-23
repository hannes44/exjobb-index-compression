package org.apache.lucene.codecs.exjobb.integercompression;

/**
 * Enum for the different types of integer compression.
 */
public enum IntegerCompressionType {
    DELTA,
    FOR,
    PFOR,
    NONE,
    ELIASGAMMA,
    ELIASFANO,
    FASTPFOR,
    DEFAULT,
    LIMITTTEST,
    LIMITTEST2,
    SIMPLE8B,
    NEWPFOR,
    OPTIMALFASTPFOR
}
