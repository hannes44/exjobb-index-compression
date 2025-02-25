package org.apache.lucene.codecs.lucene912;

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
    LIMITTEST2
}
