/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

/**
 * The validation exception for Kafka Consumer Side Offset Store.
 * This exception is used for validation failures.
 */
public class OffsetStoreValidationException extends OffsetStoreException {
    public OffsetStoreValidationException(String message) {
        super(message);
    }
}
