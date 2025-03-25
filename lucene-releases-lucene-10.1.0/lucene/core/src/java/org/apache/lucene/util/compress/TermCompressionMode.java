/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.util.compress;

import org.apache.lucene.util.compress.unsafeSnappy.UnsafeSnappy;
import org.apache.lucene.util.compress.unsafeZstd.UnsafeZSTD;

/** Configuration for the compression mode of the terms */
public enum TermCompressionMode {
    /** No compression */
    NO_COMPRESSION,
    /** Compress terms with {@link LowercaseAsciiCompression} */
    LOWERCASE_ASCII,
    /** Compress terms with {@link LZ4} */
    LZ4,
    /** Compress terms with Zstandard {@link UnsafeZSTD} */
    ZSTD,
    /** Compress terms with Snappy {@link UnsafeSnappy} */
    SNAPPY
}
