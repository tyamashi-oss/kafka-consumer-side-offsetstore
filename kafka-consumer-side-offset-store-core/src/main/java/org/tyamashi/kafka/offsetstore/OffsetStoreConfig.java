/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration for Kafka Consumer Side Offset Store
 */
public class OffsetStoreConfig {
    private static final Logger logger = LoggerFactory.getLogger(OffsetStoreConfig.class);

    /**
     * Indicates whether kafka side offset commit is enabled. "true" if kafka side offset commit is enabled, or "false" otherwise. Default is "true".
     */
    public static final String ENABLE_KAFKA_SIDE_OFFSET_COMMIT = "enable.kafkasideoffset.commit";
    private static final boolean DEFAULT_ENABLE_KAFKA_SIDE_OFFSET_COMMIT = true;

    /**
     * If kafka side offset commit is enabled (enable.kafkasideoffset.commit = true), strategy to update Kafka side offset. Default is "commit.async.with.updating.consumerside.offsets".
     */
    public static final String KAFKA_SIDE_OFFSET_COMMIT_STRATEGY = "kafkaside.offset.commit.strategy";
    private static final String DEFAULT_KAFKA_SIDE_OFFSET_COMMIT_STRATEGY = KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS;

    /**
     * Whether to user __consumer_offsets on the Kafka cluster when the offsets cannot be found from the consumer side offset store. Default is "use.kafka.offsets".
     */
    public static final String OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY = "offsetnotfound.on.consumerside.strategy";
    private static final String DEFAULT_OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY = OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS;

    /**
     * Indicates whether kafka consumer timeout check is enabled. This is one of the zombie fencing features. "true" if kafka consumer timeout check is enabled, or "false" otherwise. Default is "true".
     */
    public static final String ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK = "enable.kafka.consumer.timeout.check";
    private static final boolean DEFAULT_ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK = true;

    /**
     * original config values passed
     */
    private final Map<String, Object> originals;

    /**
     * config values adjusted
     */
    private final Map<String, Object> values;

    /**
     * Construct a configuration for Kafka Consumer Side Offset Store
     * @param props original config values
     * @throws OffsetStoreValidationException if invalid values exist in props
     */
    public OffsetStoreConfig(Map<String, Object> props) throws OffsetStoreValidationException {
        this.originals = new HashMap<>(props); // copy original map

        this.values = new HashMap<>();

        this.values.put(ENABLE_KAFKA_SIDE_OFFSET_COMMIT, Boolean.parseBoolean(Optional.ofNullable(this.originals.get(ENABLE_KAFKA_SIDE_OFFSET_COMMIT)).orElse(DEFAULT_ENABLE_KAFKA_SIDE_OFFSET_COMMIT).toString()));

        String kafkaSideOffsetCommitStrategy = Optional.ofNullable(this.originals.get(KAFKA_SIDE_OFFSET_COMMIT_STRATEGY)).orElse(DEFAULT_KAFKA_SIDE_OFFSET_COMMIT_STRATEGY).toString();
        if ((kafkaSideOffsetCommitStrategy.equals(KafkaSideOffsetCommitStrategy.COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS) || kafkaSideOffsetCommitStrategy.equals(KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS)) == false) {
            throw new OffsetStoreValidationException(String.format("'%s' is invalid value [original=%s, valid values=%s]", KAFKA_SIDE_OFFSET_COMMIT_STRATEGY , originals.get(KAFKA_SIDE_OFFSET_COMMIT_STRATEGY), Arrays.asList(KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS, KafkaSideOffsetCommitStrategy.COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS)));
        }
        this.values.put(KAFKA_SIDE_OFFSET_COMMIT_STRATEGY, kafkaSideOffsetCommitStrategy);

        String offsetNotFoundOnConsumerSideStrategy = Optional.ofNullable(this.originals.get(OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY)).orElse(DEFAULT_OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY).toString();
        if ((offsetNotFoundOnConsumerSideStrategy.equals(OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS) || offsetNotFoundOnConsumerSideStrategy.equals(OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS)) == false) {
            throw new OffsetStoreValidationException(String.format("'%s' is invalid value [original=%s, valid values=%s]", OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY , originals.get(OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY), Arrays.asList(OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS, OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS )));
        }
        this.values.put(OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, offsetNotFoundOnConsumerSideStrategy);

        this.values.put(ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK, Boolean.parseBoolean(Optional.ofNullable(this.originals.get(ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK)).orElse(DEFAULT_ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK).toString()));

        logger.info("OffsetStoreConfig [{}]", this.values);
    }

    /**
     * enable.kafkasideoffset.commit
     * @return enable.kafkasideoffset.commit
     */
    public boolean isEnableKafkaSideOffsetCommit() {
        return (boolean) values.get(ENABLE_KAFKA_SIDE_OFFSET_COMMIT);
    }

    /**
     * kafkaside.offset.commit.strategy
     * @return kafkaside.offset.commit.strategy
     */
    public String getKafkaSideOffsetCommitStrategy() {
        return (String) values.get(KAFKA_SIDE_OFFSET_COMMIT_STRATEGY);
    }

    /**
     * offsetnotfound.on.consumerside.strategy
     * @return offsetnotfound.on.consumerside.strategy
     */
    public String getOffsetNotFoundOnConsumerSideStrategy() {
        return (String) values.get(OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY);
    }

    /**
     * enable.kafka.consumer.timeout.check
     * @return enable.kafka.consumer.timeout.check
     */
    public boolean isEnableKafkaConsumerTimeoutCheck() {
        return (boolean) values.get(ENABLE_KAFKA_CONSUMER_TIMEOUT_CHECK);
    }

    /**
     * If kafka side offset commit is enabled (enable.kafkasideoffset.commit=true), strategy to update Kafka side offset.
     */
    public static class KafkaSideOffsetCommitStrategy {
        /**
         * The Kafka side offsets is committed with commitAsync when updating consumer side offsets.
         */
        public static String COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS = "commit.async.with.updating.consumerside.offsets";

        /**
         * The Kafka side offsets is committed with commitSync when updating consumer side offsets.
         */
        public static String COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS = "commit.sync.with.updating.consumerside.offsets";
    }

    /**
     * Whether to use __consumer_offsets on the Kafka cluster when the offsets cannot be found form the consumer side offset store
     */
    public static class OffsetNotFoundOnConsumerSideStrategy {
        /**
         * Use __consumer_offsets on the Kafka cluster. if not even in __consumer_offsets, offsets based on auto.offset.reset would be used.
         * @see "https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset"
         */
        public static String USE_KAFKA_OFFSETS = "use.kafka.offsets";

        /**
         * Ignore __consumer_offsets on the Kafka cluster. And offsets based on auto.offset.reset would be used.
         * @see "https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset"
         */
        public static String IGNORE_KAFKA_OFFSETS = "ignore.kafka.offsets";
    }
}
