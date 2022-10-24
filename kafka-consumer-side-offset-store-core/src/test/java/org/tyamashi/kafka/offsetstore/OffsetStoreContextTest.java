/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package org.tyamashi.kafka.offsetstore;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

@EmbeddedKafka(count = 1, partitions = OffsetStoreContextTest.PARTITION)
@ExtendWith(SpringExtension.class)
public class OffsetStoreContextTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private static final Logger logger = LoggerFactory.getLogger(OffsetStoreContext.class);

    public static final String GROUP = "my-group";
    public final static int PARTITION = 5;

    @Test
    public void testSimple() throws Exception {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i)).get();
        }
        producer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(0));

        List<Future> results = TestUtils.consumeMessages(messageCount, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler());

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 10L);
        assetOffsets.put(new TopicPartition(topicName, 1), 10L);
        assetOffsets.put(new TopicPartition(topicName, 2), 10L);
        assetOffsets.put(new TopicPartition(topicName, 3), 10L);
        assetOffsets.put(new TopicPartition(topicName, 4), 10L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);

    }

    @Test
    public void testRandomRollbackException() throws Exception {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i)).get();
        }
        producer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(0));
        initializeOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(0));

        List<Future> results = TestUtils.consumeMessages(messageCount, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler(), 30, true);

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 10L);
        assetOffsets.put(new TopicPartition(topicName, 1), 10L);
        assetOffsets.put(new TopicPartition(topicName, 2), 10L);
        assetOffsets.put(new TopicPartition(topicName, 3), 10L);
        assetOffsets.put(new TopicPartition(topicName, 4), 10L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);

    }

    @Test
    public void testOffsetOutOfRangeWithAutoOffsetResetNone() throws Throwable {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }
        producer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(100));

        List<Future> results = TestUtils.consumeMessages(1, GROUP, topicName, 1, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler());

        try {
            results.get(0).get();
            fail();
        } catch (ExecutionException e) {
            try {
                // NoOffsetForPartitionException or OffsetOutOfRangeException will occur. It depends on the Kafka version with exception is occurred.
                throw e.getCause();
            } catch (NoOffsetForPartitionException e1) {
                // success
            } catch (OffsetOutOfRangeException e2) {
                // success
            } catch (Throwable t) {
                fail();
            }
        }
    }

    @Test
    public void testOffsetOutOfRangeWithAutoOffsetResetEarliest() throws Throwable {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }
        producer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(100L));
        initializeOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(100L));
        initializeOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(100L));
        initializeOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(100L));
        initializeOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(100L));

        List<Future> results = TestUtils.consumeMessages(messageCount, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler());

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 10L);
        assetOffsets.put(new TopicPartition(topicName, 1), 10L);
        assetOffsets.put(new TopicPartition(topicName, 2), 10L);
        assetOffsets.put(new TopicPartition(topicName, 3), 10L);
        assetOffsets.put(new TopicPartition(topicName, 4), 10L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);
    }

    @Test
    public void testOffsetOutOfRangeWithAutoOffsetResetLatest() throws Throwable {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(100));
        initializeOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(100));

        List<Future> results = TestUtils.consumeMessages(messageCount, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler(), 0, false);

        // wait for consumers to seek latest offset
        Thread.sleep(3000);

        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }
        producer.close();

        results.stream().forEach(a -> {
            try {
                a.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 20L);
        assetOffsets.put(new TopicPartition(topicName, 1), 20L);
        assetOffsets.put(new TopicPartition(topicName, 2), 20L);
        assetOffsets.put(new TopicPartition(topicName, 3), 20L);
        assetOffsets.put(new TopicPartition(topicName, 4), 20L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);

    }

    @Test
    public void testSlowConsumerOverMaxPollInterval() throws Exception {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 5;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i)).get();
        }
        producer.close();

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 7000);

        Map<String, Object> offsetStoreProps = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        initializeOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(0));

        List<Future> slowResults = TestUtils.consumeMessages(messageCount, GROUP, topicName, 1, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler((connection) -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }), 0, false);

        List<Future> normalResults = TestUtils.consumeMessages(messageCount, GROUP, topicName, 5, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler(), 0, false);

        normalResults.stream().forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        try {
            slowResults.get(0).get();
            fail();
        } catch (Exception e) {
            if (e.getCause().getClass().equals(OffsetStoreValidationException.class) == false) {
                fail();
            }
        }

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 1L);
        assetOffsets.put(new TopicPartition(topicName, 1), 1L);
        assetOffsets.put(new TopicPartition(topicName, 2), 1L);
        assetOffsets.put(new TopicPartition(topicName, 3), 1L);
        assetOffsets.put(new TopicPartition(topicName, 4), 1L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);
    }

    @Test
    public void testNotFoundOffsetsWithAutoOffsetResetLatestAndIgnoreKafkaOffsets() throws Throwable {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Map<String, Object> offsetStoreProps = new HashMap<>();
        offsetStoreProps.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.IGNORE_KAFKA_OFFSETS);

        try (AdminClient adminClient = TestUtils.getAdminClient(this.embeddedKafkaBroker)) {
            Map<TopicPartition, OffsetAndMetadata> kafkaSideOffsets = new HashMap<>();
            kafkaSideOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(5));
            kafkaSideOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(5));
            kafkaSideOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(5));
            kafkaSideOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(5));
            kafkaSideOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(5));

            adminClient.alterConsumerGroupOffsets(GROUP, kafkaSideOffsets).all().get(10, TimeUnit.SECONDS);
        }

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<>();


        List<Future> results = TestUtils.consumeMessages(messageCount, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler(), 0, false);

        // wait for consumers to seek latest offset
        Thread.sleep(3000);

        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }
        producer.close();

        results.stream().forEach(a -> {
            try {
                a.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 20L);
        assetOffsets.put(new TopicPartition(topicName, 1), 20L);
        assetOffsets.put(new TopicPartition(topicName, 2), 20L);
        assetOffsets.put(new TopicPartition(topicName, 3), 20L);
        assetOffsets.put(new TopicPartition(topicName, 4), 20L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);

    }

    @Test
    public void testNotFoundOffsetsWithAutoOffsetResetLatestAndUseKafkaOffsets() throws Throwable {
        SimpleJdbcTestUtils.initializeDatabase();

        String topicName = new Object() {
        }.getClass().getEnclosingMethod().getName().toLowerCase(Locale.ROOT);
        this.embeddedKafkaBroker.addTopics(topicName);

        int messageCount = 50;

        KafkaProducer<Integer, Integer> producer = TestUtils.getSimpleKafkaProducer(embeddedKafkaBroker);
        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }

        final Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps(GROUP, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Map<String, Object> offsetStoreProps = new HashMap<>();
        offsetStoreProps.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS);

        Map<TopicPartition, OffsetAndMetadata> kafkaSideOffsets = new HashMap<>();
        kafkaSideOffsets.put(new TopicPartition(topicName, 0), new OffsetAndMetadata(5));
        kafkaSideOffsets.put(new TopicPartition(topicName, 1), new OffsetAndMetadata(5));
        kafkaSideOffsets.put(new TopicPartition(topicName, 2), new OffsetAndMetadata(5));
        kafkaSideOffsets.put(new TopicPartition(topicName, 3), new OffsetAndMetadata(5));
        kafkaSideOffsets.put(new TopicPartition(topicName, 4), new OffsetAndMetadata(5));

        try (AdminClient adminClient = TestUtils.getAdminClient(this.embeddedKafkaBroker)) {
            adminClient.alterConsumerGroupOffsets(GROUP, kafkaSideOffsets).all().get(10, TimeUnit.SECONDS);
        }

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets = new HashMap<>();


        List<Future> results = TestUtils.consumeMessages(messageCount + messageCount/2, GROUP, topicName, PARTITION, consumerProps, offsetStoreProps, new SimpleJdbcTestUtils.ConnectionConsumerSideOffsetStoreHandler(initializeOffsets), SimpleJdbcTestUtils.getTransactioncontextHandler(), 0, false);

        // wait for consumers to seek latest offset
        Thread.sleep(3000);

        for (int i = 0; i < messageCount; ++i) {
            producer.send(new ProducerRecord<Integer, Integer>(topicName, i % PARTITION, i));
        }
        producer.close();

        results.stream().forEach(a -> {
            try {
                a.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Map<TopicPartition, Long> assetOffsets = new HashMap<>();
        assetOffsets.put(new TopicPartition(topicName, 0), 20L);
        assetOffsets.put(new TopicPartition(topicName, 1), 20L);
        assetOffsets.put(new TopicPartition(topicName, 2), 20L);
        assetOffsets.put(new TopicPartition(topicName, 3), 20L);
        assetOffsets.put(new TopicPartition(topicName, 4), 20L);
        SimpleJdbcTestUtils.assertOffsets(this.embeddedKafkaBroker, GROUP, assetOffsets);

    }
}
