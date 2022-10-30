/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.Heartbeat;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * OffsetStoreContext is the pivotal object for controlling and configuring Kafka Consumer Side Offsets.
 * @param <TRANSACTIONALOBJECT> transactional object type. For example, JDBC connection, etc.
 */
public class OffsetStoreContext<TRANSACTIONALOBJECT> {

    private static final Logger logger = LoggerFactory.getLogger(OffsetStoreContext.class);

    private static Field coordinatorField;
    private static Field heartbeatField;
    private static Field sessionTimerField;
    private static Field pollTimerField;

    static {
        // Note: This is robbery viewing private field. These fields will be used when "enable.kafka.consumer.timeout.check=true"
        try {
            coordinatorField = KafkaConsumer.class.getDeclaredField("coordinator");
            coordinatorField.setAccessible(true);
            heartbeatField = AbstractCoordinator.class.getDeclaredField("heartbeat");
            heartbeatField.setAccessible(true);
            sessionTimerField = Heartbeat.class.getDeclaredField("sessionTimer");
            sessionTimerField.setAccessible(true);
            pollTimerField = Heartbeat.class.getDeclaredField("pollTimer");
            pollTimerField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final KafkaConsumer kafkaConsumer;

    private AbstractCoordinator coordinator;
    private Heartbeat heartbeat;
    private Timer sessionTimer;
    private Timer pollTimer;


    private final ConsumerSideOffsetStoreHandler handler;

    private final OffsetStoreConfig offsetStoreConfig;

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(pattern).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param pattern Pattern passing into KafkaConsumer.subscription(pattern).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     * @return OffsetStoreContext.
     * @param <TRANSACTIONALOBJECT> transactional object type. For example, JDBC connection, etc.
     */
    public static <TRANSACTIONALOBJECT> OffsetStoreContext<TRANSACTIONALOBJECT> subscribe(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Pattern pattern, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        return new OffsetStoreContext(offsetStoreProps, kafkaConsumer, pattern, consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(topics).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param topics Topics passing into KafkaConsumer.subscription(topics).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     * @return OffsetStoreContext.
     * @param <TRANSACTIONALOBJECT> transactional object type. For example, JDBC connection, etc.
     */
    public static <TRANSACTIONALOBJECT> OffsetStoreContext<TRANSACTIONALOBJECT> subscribe(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Collection<String> topics, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        return new OffsetStoreContext(offsetStoreProps, kafkaConsumer, topics, consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(pattern, listener).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param pattern Pattern passing into KafkaConsumer.subscription(pattern, listener).
     * @param listener ConsumerRebalanceListener instance passing into KafkaConsumer.subscription(pattern, listener).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     * @return OffsetStoreContext.
     * @param <TRANSACTIONALOBJECT> transactional object type. For example, JDBC connection, etc.
     */
    public static <TRANSACTIONALOBJECT> OffsetStoreContext<TRANSACTIONALOBJECT> subscribe(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Pattern pattern, ConsumerRebalanceListener listener, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        return new OffsetStoreContext(offsetStoreProps, kafkaConsumer, pattern, listener, consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(topics, listener).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param topics Topics passing into KafkaConsumer.subscription(topics, listener).
     * @param listener ConsumerRebalanceListener instance passing into KafkaConsumer.subscription(topics, listener).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     * @return OffsetStoreContext.
     * @param <TRANSACTIONALOBJECT> transactional object type. For example, JDBC connection, etc.
     */
    public static <TRANSACTIONALOBJECT> OffsetStoreContext<TRANSACTIONALOBJECT> subscribe(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Collection<String> topics, ConsumerRebalanceListener listener, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        return new OffsetStoreContext(offsetStoreProps, kafkaConsumer, topics, listener, consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(pattern).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param pattern Pattern passing into KafkaConsumer.subscription(pattern).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     */
    public OffsetStoreContext(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Pattern pattern, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        this(offsetStoreProps, kafkaConsumer, pattern, new NoOpConsumerRebalanceListener(), consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(topics).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param topics Topics passing into KafkaConsumer.subscription(topics).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     */
    public OffsetStoreContext(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Collection<String> topics, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        this(offsetStoreProps, kafkaConsumer, topics, new NoOpConsumerRebalanceListener(), consumerSideOffsetStoreHandler);
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(pattern, listener).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param pattern Pattern passing into KafkaConsumer.subscription(pattern, listener).
     * @param listener ConsumerRebalanceListener instance passing into KafkaConsumer.subscription(pattern, listener).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     */
    public OffsetStoreContext(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Pattern pattern, ConsumerRebalanceListener listener, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        validateConsumer(kafkaConsumer);
        this.offsetStoreConfig = new OffsetStoreConfig(offsetStoreProps);
        this.kafkaConsumer = kafkaConsumer;
        cacheConsumerPrivateFields(this.kafkaConsumer);
        this.handler = consumerSideOffsetStoreHandler;
        initialize();
        kafkaConsumer.subscribe(pattern, new OffsetStoreConsumerRebalanceListener(listener));
        logger.debug("The subscribe was called [KafkaConsumer={}, consumerSideOffsetStoreHandler={}]", this.kafkaConsumer, OffsetStoreContext.this.handler.getClass().getSimpleName());
    }

    /**
     * Create OffsetStoreContext with offsetStoreHandler, then call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...) and KafkaConsumser.subscribe(topics, listener).
     * @param offsetStoreProps The configuration properties for Kafka Consumer Side Offset Store.
     * @param kafkaConsumer KafkaConsumer
     * @param topics Topics passing into KafkaConsumer.subscription(topics, listener).
     * @param listener ConsumerRebalanceListener instance passing into KafkaConsumer.subscription(topics, listener).
     * @param consumerSideOffsetStoreHandler ConsumerSideOffsetStoreHandler.
     */
    public OffsetStoreContext(Map<String, Object> offsetStoreProps, KafkaConsumer kafkaConsumer, Collection<String> topics, ConsumerRebalanceListener listener, ConsumerSideOffsetStoreHandler<TRANSACTIONALOBJECT> consumerSideOffsetStoreHandler) {
        validateConsumer(kafkaConsumer);
        this.offsetStoreConfig = new OffsetStoreConfig(offsetStoreProps);
        this.kafkaConsumer = kafkaConsumer;
        cacheConsumerPrivateFields(this.kafkaConsumer);
        this.handler = consumerSideOffsetStoreHandler;
        initialize();
        kafkaConsumer.subscribe(topics, new OffsetStoreConsumerRebalanceListener(listener));
        logger.debug("The subscribe was called [KafkaConsumer={}, handler={}]", this.kafkaConsumer, OffsetStoreContext.this.handler.getClass().getSimpleName());
    }

    /**
     * Cache consumer private fields once, to access immediately.
     * @param kafkaConsumer KafkaConsumer
     */
    private void cacheConsumerPrivateFields(KafkaConsumer kafkaConsumer) {
        if (this.offsetStoreConfig.isEnableKafkaConsumerTimeoutCheck()) {
            try {
                this.coordinator = (AbstractCoordinator) coordinatorField.get(kafkaConsumer);
                this.heartbeat = (Heartbeat) heartbeatField.get(this.coordinator);
                this.sessionTimer = (Timer) sessionTimerField.get(this.heartbeat);
                this.pollTimer = (Timer) pollTimerField.get(this.heartbeat);
            } catch (IllegalAccessException e) {
                throw new OffsetStoreValidationException("The coordinator field or the heartbeat field in KafkaConsumer can not be accessed [kafkaConsumer=" + kafkaConsumer + "]");
            }
        }
    }

    /**
     * Validate the KafkaConsumer instance.
     * @param kafkaConsumer KafkaConsumer
     */
    private void validateConsumer(KafkaConsumer kafkaConsumer) {
        // check groupId
        try {
            kafkaConsumer.groupMetadata(); // if group.id is not set. InvalidGroupIdException will occur.
        } catch (InvalidGroupIdException e) {
            throw new OffsetStoreValidationException("The consumer group id should not be null [kafkaConsumer=" + kafkaConsumer + "]", e);
        }
    }

    /**
     * Initialize OffsetStoreContext, this will call ConsumerSideOffsetStoreHandler.initializeOffsetStore(...).
     * @throws OffsetStoreExecutionException This exception wraps exception thrown in ConsumerSideOffsetStoreHandler.initializeOffsetStore(...).
     */
    private void initialize() throws OffsetStoreExecutionException {
        try {
            logger.debug("Calling your initializeOffsetStore [handler={}, kafkaConsumer={}]",OffsetStoreContext.this.handler.getClass().getSimpleName() ,this.kafkaConsumer);
            handler.initializeOffsetStore(kafkaConsumer.groupMetadata().groupId());
        } catch (Exception e) {
            throw new OffsetStoreExecutionException(e);
        }
    }

    /**
     * Update the consumer side offsets, this will call ConsumerSideOffsetStoreHandler.saveOffsets(...). This method should be called just before commit() if possible.
     * @param records Consumer records that lead the consumer side offsets.
     * @param transactionalObject Transactional object. For example, JDBC connection, etc.
     * @throws OffsetStoreExecutionException OffsetStoreExecutionException wrap an exception that occurs in ConsumerSideOffsetStoreHandler.saveOffsets(...). You can get the original exception from OffsetStoreExecutionException.getCause().
     * @throws OffsetStoreValidationException This exception will occur when KafkaConsumer may be revoked from the consumer group.
     */
    public void updateConsumerSideOffsets(ConsumerRecords<?, ?> records, TRANSACTIONALOBJECT transactionalObject) throws OffsetStoreExecutionException, OffsetStoreValidationException {
        List<ConsumerRecord<?, ?>> recordList = new ArrayList<ConsumerRecord<?, ?>>();
        records.iterator().forEachRemaining(recordList::add);
        updateConsumerSideOffsets(recordList, transactionalObject);
    }

    /**
     * Update the consumer side offsets, this will call ConsumerSideOffsetStoreHandler.saveOffsets(...). This method should be called just before commit() if possible.
     * @param records Consumer records that lead the consumer side offsets.
     * @param transactionalObject Transactional object. For example, JDBC connection, etc.
     * @throws OffsetStoreExecutionException OffsetStoreExecutionException wrap an exception that occurs in ConsumerSideOffsetStoreHandler.saveOffsets(...). You can get the original exception from OffsetStoreExecutionException.getCause().
     * @throws OffsetStoreValidationException This exception will occur when KafkaConsumer may be revoked from the consumer group.
     */
    public void updateConsumerSideOffsets(Collection<ConsumerRecord<?, ?>> records, TRANSACTIONALOBJECT transactionalObject) throws OffsetStoreExecutionException, OffsetStoreValidationException {
        Map<TopicPartition, OffsetAndMetadata> offsets = translateToTopicPartitionOffsetAndMetadataMap(records);
        updateConsumerSideOffsets(offsets, transactionalObject);
    }

    /**
     * Update the consumer side offsets, this will call ConsumerSideOffsetStoreHandler.saveOffsets(...). This method should be called just before commit() if possible.
     * @param record Consumer record that lead the consumer side offsets.
     * @param transactionalObject Transactional object. For example, JDBC connection, etc.
     * @throws OffsetStoreExecutionException OffsetStoreExecutionException wrap an exception that occurs in ConsumerSideOffsetStoreHandler.saveOffsets(...). You can get the original exception from OffsetStoreExecutionException.getCause().
     * @throws OffsetStoreValidationException This exception will occur when KafkaConsumer may be revoked from the consumer group.
     */
    public void updateConsumerSideOffsets(ConsumerRecord<?, ?> record, TRANSACTIONALOBJECT transactionalObject) throws OffsetStoreExecutionException, OffsetStoreValidationException {
        updateConsumerSideOffsets(Collections.singletonList(record), transactionalObject);
    }

    /**
     * Update the consumer side offsets, this will call ConsumerSideOffsetStoreHandler.saveOffsets(...). This method should be called just before commit() if possible.
     * @param offsets The offsets to be updated.
     * @param transactionalObject Transactional object. For example, JDBC connection, etc.
     * @throws OffsetStoreExecutionException OffsetStoreExecutionException wrap an exception that occurs in ConsumerSideOffsetStoreHandler.saveOffsets(...). You can get the original exception from OffsetStoreExecutionException.getCause().
     * @throws OffsetStoreValidationException This exception will occur when KafkaConsumer may be revoked from the consumer group.
     */
    public void updateConsumerSideOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, TRANSACTIONALOBJECT transactionalObject) throws OffsetStoreExecutionException, OffsetStoreValidationException {
        try {
            ConsumerGroupMetadata savedGroupMetadata = this.kafkaConsumer.groupMetadata();
            checkConsumerTimeoutMs();
            
            if (offsetStoreConfig.isEnableKafkaSideOffsetCommit()) {
                if (offsetStoreConfig.getKafkaSideOffsetCommitStrategy().equals(OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS)) {
                    commitAsyncKafkaSideOffsets(offsets);
                } else if (offsetStoreConfig.getKafkaSideOffsetCommitStrategy().equals(OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_SYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS)) {
                    commitSyncKafkaSideOffsets(offsets);
                }
            }
            logger.debug("Calling your saveOffsets with [handler={}, groupMetadata={}, offsets={}, transactionalObject={}].", OffsetStoreContext.this.handler.getClass().getSimpleName(), OffsetStoreContext.this.kafkaConsumer.groupMetadata() ,offsets, transactionalObject);
            handler.saveOffsets(OffsetStoreContext.this.kafkaConsumer.groupMetadata(), offsets, transactionalObject);

            checkConsumerTimeoutMs();

            if (savedGroupMetadata.equals(this.kafkaConsumer.groupMetadata()) == false) {
                throw new OffsetStoreValidationException(String.format("The consumer group metadata was changed during updateConsumerSideOffsets() [old group metadata=%d, current group metadata=%d]", savedGroupMetadata, this.kafkaConsumer.groupMetadata()));
            }
        } catch (Exception e) {
            if (e instanceof OffsetStoreValidationException) {
                throw (OffsetStoreValidationException) e;
            } else {
                throw new OffsetStoreExecutionException(e);
            }
        }
    }

    /**
     * Check KafkaConsumser's session timeout and poll timeout. If any of these expire, OffsetStoreValidationException occurs.
     * @return The remaining timeout value in milliseconds. The smaller remaining timeout value of KafkaConsumser's session timeout or poll timeout.
     * @throws OffsetStoreValidationException If KafkaConsumser's session timeout or poll timeout expired.
     */
    public long checkConsumerTimeoutMs() throws OffsetStoreValidationException {
        if (offsetStoreConfig.isEnableKafkaConsumerTimeoutCheck()) {
            long sessionRemainingMs = sessionTimer.remainingMs() - (Time.SYSTEM.milliseconds() - sessionTimer.currentTimeMs());
            long pollRemainingMs = pollTimer.remainingMs() - (Time.SYSTEM.milliseconds() - pollTimer.currentTimeMs());

            if (sessionRemainingMs <= 0) {
                throw new OffsetStoreValidationException(String.format("The consumer session timeout of %sms expired, based on 'session.timeout.ms'", sessionTimer.timeoutMs()));
            }

            if (pollRemainingMs <= 0) {
                throw new OffsetStoreValidationException(String.format("The poll timeout of %sms expired, based on 'max.poll.interval.ms'", pollTimer.timeoutMs()));
            }

            return Math.max(0, Math.min(sessionRemainingMs, pollRemainingMs));
        } else {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitSync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitSync(...);
     * @param records Consumer records that lead the offsets.
     */
    public void commitSyncKafkaSideOffsets(Collection<ConsumerRecord<?, ?>> records)  {
        Map<TopicPartition, OffsetAndMetadata> offsets = translateToTopicPartitionOffsetAndMetadataMap(records);
        commitSyncKafkaSideOffsets(offsets);
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitSync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitSync(...);
     * @param record Consumer record that lead the offsets.
     */
    public void commitSyncKafkaSideOffsets(ConsumerRecord<?, ?> record) {
        commitSyncKafkaSideOffsets(Collections.singletonList(record));
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitSync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitSync(...);
     * @param offsets The offsets to be updated.
     */
    public void commitSyncKafkaSideOffsets(Map<TopicPartition, OffsetAndMetadata> offsets)  {
        logger.debug("Calling commitSyncKafkaSideOffsets [groupMetadata={}, offsets={}].", OffsetStoreContext.this.kafkaConsumer.groupMetadata() ,offsets);
        kafkaConsumer.commitSync(offsets);
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitAsync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitAsync(...);
     * @param records Consumer records that lead the offsets.
     */
    public void commitAsyncKafkaSideOffsets(Collection<ConsumerRecord<?, ?>> records)  {
        Map<TopicPartition, OffsetAndMetadata> offsets = translateToTopicPartitionOffsetAndMetadataMap(records);
        commitAsyncKafkaSideOffsets(offsets);
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitAsync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitAsync(...);
     * @param record Consumer record that lead the offsets.
     */
    public void commitAsyncKafkaSideOffsets(ConsumerRecord<?, ?> record) {
        commitAsyncKafkaSideOffsets(Collections.singletonList(record));
    }

    /**
     * Commit offsets to Kafka side (i.e. __consumer_offsets) using KakfaConsumer.commitAsync(...).
     * This method may also throw exceptions occur in KafkaConsumer.commitAsync(...);
     * @param offsets The offsets to be updated.
     */
    public void commitAsyncKafkaSideOffsets(Map<TopicPartition, OffsetAndMetadata> offsets)  {
        logger.debug("Calling commitAsyncKafkaSideOffsets [groupMetadata={}, offsets={}].", OffsetStoreContext.this.kafkaConsumer.groupMetadata() ,offsets);
        kafkaConsumer.commitAsync(offsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    logger.warn("kafka side async commit failed [offsets={}]", exception);
                }
            }
        });
    }

    /**
     * Translate records to a map of offsets by partition with associate metadata.
     * @param records Consumer records that lead the offsets.
     * @return A map of offsets by partition with associate metadata.
     */
    private Map<TopicPartition, OffsetAndMetadata> translateToTopicPartitionOffsetAndMetadataMap(Collection<ConsumerRecord<?, ?>> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (ConsumerRecord record: records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());

            long previousOffsetForStore = -1;
            OffsetAndMetadata previousOffsetAndMetadata = offsets.get(topicPartition);
            if (previousOffsetAndMetadata != null) {
                previousOffsetForStore = previousOffsetAndMetadata.offset();
            }

            long currentOffsetForStore = record.offset() + 1;
            if (currentOffsetForStore > previousOffsetForStore) {
                offsets.put(topicPartition, new OffsetAndMetadata(currentOffsetForStore));
            }
        }
        return offsets;
    }

    /**
     * OffsetStoreConsumerRebalanceListener instance is passed to KafkaConsumer.subscribe(...), to listen to consumer rebalancing events.
     */
    private class OffsetStoreConsumerRebalanceListener implements ConsumerRebalanceListener {

        private final ConsumerRebalanceListener listenerDefinedByUser;

        public OffsetStoreConsumerRebalanceListener(ConsumerRebalanceListener listenerDefinedByUser) {
            this.listenerDefinedByUser = listenerDefinedByUser;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignedTopicPartitions) {
            if (logger.isDebugEnabled()) {
                logger.debug("onPartitionsAssigned: [{}]", Utils.join(assignedTopicPartitions, ", "));
            }

            Map<TopicPartition, OffsetAndMetadata> loadedOffsets;
            try {
                loadedOffsets = Optional.ofNullable(OffsetStoreContext.this.handler.loadOffsets(OffsetStoreContext.this.kafkaConsumer.groupMetadata(), assignedTopicPartitions)).orElse(Collections.EMPTY_MAP);
                logger.debug("Your loadOffsets is called [handler={}, return={}].", OffsetStoreContext.this.handler.getClass().getSimpleName(), loadedOffsets);
            } catch (Exception e) {
                throw new OffsetStoreExecutionException(e);
            }

            if (logger.isWarnEnabled()) {
                List<TopicPartition> unassignedTopicPartitionsInResult = loadedOffsets.entrySet().stream().filter(entry -> assignedTopicPartitions.contains(entry.getKey()) == false).map(Map.Entry::getKey).collect(Collectors.toList());
                if (unassignedTopicPartitionsInResult.size() > 0) {
                    logger.warn("Your loadOffsets returns unassigned TopicPartition. It will ignored. [handler={}, topicPartition={}, groupMetadata={}]", OffsetStoreContext.this.handler.getClass().getSimpleName(), Utils.join(unassignedTopicPartitionsInResult, ", "), OffsetStoreContext.this.kafkaConsumer.groupMetadata());
                }
            }

            // seek to loadedOffsets if assigned TopicPartition
            List<Map.Entry<TopicPartition, OffsetAndMetadata>> seekOffsets = loadedOffsets.entrySet().stream().filter(entry -> assignedTopicPartitions.contains(entry.getKey())).collect(Collectors.toList());
            seekOffsets.stream().forEach(entry -> OffsetStoreContext.this.kafkaConsumer.seek(entry.getKey(), entry.getValue()));
            if (logger.isDebugEnabled()) {
                logger.debug("Seeked TopicPartition offsets [groupMetadata={}, TopicPartition-offsets={}]", OffsetStoreContext.this.kafkaConsumer.groupMetadata(), Utils.join(seekOffsets, ", "));
            }

            List<TopicPartition> cannotBeFoundTopicPartitionsInResult = assignedTopicPartitions.stream().filter(entry -> loadedOffsets.containsKey(entry) == false).collect(Collectors.toList());
            if (offsetStoreConfig.getOffsetNotFoundOnConsumerSideStrategy().equals(OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS)) {
                // seek to Long.MAX_VALUE to reset offset
                cannotBeFoundTopicPartitionsInResult.stream().forEach(topicPartition -> OffsetStoreContext.this.kafkaConsumer.seek(topicPartition, Long.MAX_VALUE));
            }
            if (logger.isInfoEnabled()) {
                if(cannotBeFoundTopicPartitionsInResult.size() > 0) {
                    logger.info("The offsets for assigned TopicPartition cannot be found in your loadOffset results. It will follow NotFoundOffsetsOnConsumerStrategy. [topicPartition={}, NotFoundOffsetsOnConsumerStrategy={}, groupMetadata={}]", Utils.join(cannotBeFoundTopicPartitionsInResult, ", "), offsetStoreConfig.getOffsetNotFoundOnConsumerSideStrategy(), OffsetStoreContext.this.kafkaConsumer.groupMetadata(), OffsetStoreContext.this.handler.getClass().getSimpleName());
                }
            }

            listenerDefinedByUser.onPartitionsAssigned(assignedTopicPartitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (logger.isTraceEnabled()) {
                logger.trace("onPartitionsRevoked: [{}]", Utils.join(partitions, ", "));
            }
            listenerDefinedByUser.onPartitionsRevoked(partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            if (logger.isTraceEnabled()) {
                logger.trace("onPartitionsLost: [{}]", Utils.join(partitions, ", "));
            }
            KafkaConsumer kafkaConsumer = OffsetStoreContext.this.kafkaConsumer;
            listenerDefinedByUser.onPartitionsLost(partitions);
        }
    }

}

