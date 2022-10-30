package org.tyamashi.kafka.offsetstore.internals;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.tyamashi.kafka.offsetstore.ConsumerSideOffsetStoreHandler;

import java.util.Collection;
import java.util.Map;

/**
 * No operation ConsumerSideOffsetStoreHandler
 */
public class NoOpConsumerSideOffsetStoreHandler implements ConsumerSideOffsetStoreHandler {
    @Override
    public void initializeOffsetStore(String groupId) throws Exception {
    }
    @Override
    public void saveOffsets(ConsumerGroupMetadata groupMetadata, Map offsets, Object transactionalObject) throws Exception {
    }
    @Override
    public Map<TopicPartition, OffsetAndMetadata> loadOffsets(ConsumerGroupMetadata groupMetadata, Collection collection) throws Exception {
        return null;
    }
}
