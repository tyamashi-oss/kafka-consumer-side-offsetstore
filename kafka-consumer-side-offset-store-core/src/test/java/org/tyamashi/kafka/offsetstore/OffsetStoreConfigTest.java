package org.tyamashi.kafka.offsetstore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class OffsetStoreConfigTest {
    @Test
    public void testValidation() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, true);
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "true");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "TRUE");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "True");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, false);
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "false");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "FALSE");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, "False");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaSideOffsetCommit());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, true);
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "true");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "TRUE");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "True");
        Assertions.assertTrue(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, false);
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "false");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "FALSE");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());

        props.clear();
        props.put(OffsetStoreConfig.ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, "False");
        Assertions.assertFalse(new OffsetStoreConfig(props).isEnableKafkaConsumerTimeoutCheck());


        props.clear();
        props.put(OffsetStoreConfig.KAFKA_SIDE_OFFSET_COMMIT_STRATEGY, OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS);
        Assertions.assertEquals(OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS, new OffsetStoreConfig(props).getKafkaSideOffsetCommitStrategy());

        props.clear();
        props.put(OffsetStoreConfig.KAFKA_SIDE_OFFSET_COMMIT_STRATEGY, OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS);
        Assertions.assertEquals(OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS, new OffsetStoreConfig(props).getKafkaSideOffsetCommitStrategy());

        props.clear();
        props.put(OffsetStoreConfig.KAFKA_SIDE_OFFSET_COMMIT_STRATEGY, OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS + "_XXX");
        Assertions.assertThrows(OffsetStoreValidationException.class, () -> {new OffsetStoreConfig(props);});

        props.clear();
        props.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS);
        Assertions.assertEquals(OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS, new OffsetStoreConfig(props).getOffsetNotFoundOnConsumerSideStrategy());

        props.clear();
        props.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS);
        Assertions.assertEquals(OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS, new OffsetStoreConfig(props).getOffsetNotFoundOnConsumerSideStrategy());

        props.clear();
        props.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS + "_XXX");
        Assertions.assertThrows(OffsetStoreValidationException.class, () -> {new OffsetStoreConfig(props);});


    }
}