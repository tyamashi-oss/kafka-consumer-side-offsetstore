/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

/**
 * The execution exception for Kafka Consumer Side Offset Store.
 * This exception wraps exception thrown in user code in ConsumerSideOffsetStoreHandler.
 */
public class OffsetStoreExecutionException extends OffsetStoreException {
    public OffsetStoreExecutionException(Exception e) {
        super(e);
    }
}
