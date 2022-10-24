/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

/**
 * The root exception class for Kafka Consumer Side Offset Store.
 */
public class OffsetStoreException extends RuntimeException {
    public OffsetStoreException(Exception e) {
        super(e);
    }
    public OffsetStoreException(String message) {
        super(message);
    }
}
