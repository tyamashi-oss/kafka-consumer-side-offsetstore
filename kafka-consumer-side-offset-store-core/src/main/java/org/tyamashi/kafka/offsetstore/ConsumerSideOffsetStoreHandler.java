/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Handler responsible for implementing the input/output adapter for the consumer side offset store.
 * @param <TRANSACTIONALOBJECT> The transactional object type. For example, JDBC connection, etc.
 */
public interface ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> {

    /**
     * Implement Initializing the consumer side offset store in this method. This method will be called when OffsetStoreContext is created using OffsetStoreContext.subscribe(...).
     * @param groupId The consumer group id to which the consumer belongs.
     * @throws Exception Any exception raised in this method.
     */
    void initializeOffsetStore(String groupId) throws Exception;

    /**
     * Implement loading the consumer side offsets in this method. This method will be called when partitions are assigned to the KafkaConsumer and ConsumerRebalanceListener.onPartitionsAssigned(...) is called.
     * @param groupMetadata The consumer group metadata to which the consumer belongs.
     * @param topicPartitions The topic partitions for which offsets should be loaded from the consumer side offset store.
     * @return A map of loaded offsets by the topic partition.
     * @throws Exception Any exception raised in this method.
     */
    Map<TopicPartition, OffsetAndMetadata> loadOffsets(ConsumerGroupMetadata groupMetadata, Collection<TopicPartition> topicPartitions) throws Exception;

    /**
     * Implement saving the consumer side offsets in this method. This method will be called when the application call OffsetStoreContext.updateConsumerSideOffsets(...).
     * @param groupMetadata The consumer group metadata to which the consumer belongs.
     * @param offsets The map of offsets by topic partition that should be saved in the consumer side offset store.
     * @param transactionalObject The transactional object. For example, JDBC connection, etc.
     * @throws Exception Any exception raised in this method.
     */
    void saveOffsets(ConsumerGroupMetadata groupMetadata, Map<TopicPartition, OffsetAndMetadata> offsets, TRANSACTIONALOBJECT transactionalObject) throws Exception;
}
