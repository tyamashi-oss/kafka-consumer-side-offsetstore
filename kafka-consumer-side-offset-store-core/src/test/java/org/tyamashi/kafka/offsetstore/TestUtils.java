/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static <TRANSACTIONALOBJECT extends AutoCloseable> List<Future> consumeMessages(int messageCount, String group, String topic, int consumerCount, Map<String, Object> consumerProps, Map<String, Object> offsetStoreProps, ConsumerSideOffsetStoreHandler consumerSideOffsetStoreHandler, TransactionalObjectHandler<TRANSACTIONALOBJECT> transactioncontextHandler) throws InterruptedException {
        return consumeMessages(messageCount, group, topic, consumerCount, consumerProps, offsetStoreProps, consumerSideOffsetStoreHandler, transactioncontextHandler, 10, false);
    }

    public static <TRANSACTIONALOBJECT extends AutoCloseable> List<Future> consumeMessages(int messageCount, String group, String topic, int consumerCount, Map<String, Object> consumerProps, Map<String, Object> offsetStoreProps, ConsumerSideOffsetStoreHandler consumerSideOffsetStoreHandler, TransactionalObjectHandler<TRANSACTIONALOBJECT> transactioncontextHandler, int waitSeconds, boolean enableRandomExceptin) throws InterruptedException {

        List<Future> result = new ArrayList();

        Random random = new Random();
        CountDownLatch messageCountLatch = new CountDownLatch(messageCount);
        CountDownLatch consumerCountLatch = new CountDownLatch(consumerCount);

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount);
        CompletionService<List<ConsumerRecord<Integer, String>>> completionService = new ExecutorCompletionService<List<ConsumerRecord<Integer, String>>>(executorService);

        for (int i = 0; i < consumerCount; ++i) {
            Future<?> future = completionService.submit(() -> {
                List<ConsumerRecord<Integer, String>> submitResult = new ArrayList();

                while (messageCountLatch.getCount() > 0) { // ExceptionForTest loop
                    try (KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps)) {

                        OffsetStoreContext offsetStoreContext = OffsetStoreContext.subscribe(offsetStoreProps, kafkaConsumer, Collections.singletonList(topic), consumerSideOffsetStoreHandler);

                        consumerCountLatch.countDown();
                        consumerCountLatch.await();

                        while (messageCountLatch.getCount() > 0) {
                            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                            for (ConsumerRecord<Integer, String> record : records) {
                                try (TRANSACTIONALOBJECT transactionalObject = transactioncontextHandler.getTransactionalObject()) {

                                    // something to execute in business logic
                                    transactioncontextHandler.doSomething(transactionalObject);

                                    if (enableRandomExceptin && random.nextInt(15) == 0) {
                                        throw new ExceptionForTest();
                                    }

                                    offsetStoreContext.updateConsumerSideOffsets(record, transactionalObject);

                                    transactioncontextHandler.commit(transactionalObject);
                                    offsetStoreContext.commitSyncKafkaSideOffsets(record);
                                    submitResult.add(record);
                                }
                                messageCountLatch.countDown();
                            }
                        }
                    } catch (ExceptionForTest e) {
                        logger.info("ExceptionForTest occurd");
                    } catch (Exception ex) {
                        throw ex;
                    }
                }

                return submitResult;
            });

            result.add(future);
        }

        if (waitSeconds > 0) {
            for (int i = 0; i < consumerCount; ++i) {
                Future<List<ConsumerRecord<Integer, String>>> poll = completionService.poll(waitSeconds, TimeUnit.SECONDS);
                if (poll == null) {
                    throw new RuntimeException("timeout error");
                }
            }
        }

        return result;
    }

    public static KafkaProducer<Integer, Integer> getSimpleKafkaProducer(EmbeddedKafkaBroker embeddedKafka) {
        Map<String, Object> producerProps = KafkaTestUtils
                .producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KeyPartitioner.class.getName());
        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps);
        return producer;
    }

    public interface TransactionalObjectHandler<TRANSACTIONALOBJECT> {
        TRANSACTIONALOBJECT getTransactionalObject() throws Exception;

        void commit(TRANSACTIONALOBJECT transactionalObject) throws Exception;

        void doSomething(TRANSACTIONALOBJECT transactionalObject) throws Exception;
    }

    public static AdminClient getAdminClient(EmbeddedKafkaBroker embeddedKafkaBroker) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokerAddresses()[0].toString());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-admin-client");
        return AdminClient.create(properties);
    }

    public static Map<TopicPartition, Long> getKafkaSideOffsets(EmbeddedKafkaBroker embeddedKafkaBroker, String consumerGroup, Collection<TopicPartition> targetTopicPartitions) throws InterruptedException, ExecutionException {
        try (AdminClient adminClient = TestUtils.getAdminClient(embeddedKafkaBroker)) {
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroup);
            return listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get().entrySet().stream().filter(x->targetTopicPartitions.contains(x.getKey())).collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().offset()));
        }
    }

    public static Map<TopicPartition, Long> getTopicPartionLatestOffsets(EmbeddedKafkaBroker embeddedKafkaBroker, Collection<TopicPartition> targetOffset) throws InterruptedException, ExecutionException {
        try (AdminClient adminClient = TestUtils.getAdminClient(embeddedKafkaBroker)) {
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(targetOffset.stream().collect(Collectors.toMap(x -> x, x -> OffsetSpec.latest())));
            return listOffsetsResult.all().get().entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().offset()));
        }
    }

}