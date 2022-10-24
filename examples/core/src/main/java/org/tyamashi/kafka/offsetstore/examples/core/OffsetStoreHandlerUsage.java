/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore.examples.core;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.tyamashi.kafka.offsetstore.*;
import org.tyamashi.kafka.offsetstore.OffsetStoreConfig;

import java.sql.*;
import java.time.Duration;
import java.util.*;

public class OffsetStoreHandlerUsage {
    public static final String KAFKA_CONTAINER = "confluentinc/cp-kafka:6.2.1";
    private static String TOPIC = "my-topic";
    private static String GROUP_IP = "my-group";

    public static void main(String[] args) throws Exception {
        // start Kafka broker
        KafkaContainer kafkaContainer = startKafkaBroker();

        // load dummy data to consumer side offsets store and 
        loadDummydata();

        // start producer
        startProducer(kafkaContainer);

        // configure KafkaConsumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // auto commit should be disabled
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_IP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        // configure consumer side offset store
        Map<String, Object> offsetStoreProps = new HashMap<>();
        offsetStoreProps.put(OffsetStoreConfig.OFFSET_NOT_FOUND_ON_CONSUMER_SIDE_STRATEGY, OffsetStoreConfig.OffsetNotFoundOnConsumerSideStrategy.USE_KAFKA_OFFSETS);
        offsetStoreProps.put(OffsetStoreConfig.ENABLE_KAFKA_SIDE_OFFSET_COMMIT, true);
        offsetStoreProps.put(OffsetStoreConfig.KAFKA_SIDE_OFFSET_COMMIT_STRATEGY, OffsetStoreConfig.KafkaSideOffsetCommitStrategy.COMMIT_ASYNC_WITH_UPDATING_CONSUMER_SIDE_OFFSETS);

        // define ConsumerSideOffsetStoreHandler
        ConsumerSideOffsetStoreHandler<Connection> consumerSideOffsetStoreHandler = new ConsumerSideOffsetStoreHandler<Connection>() {
            @Override
            public void initializeOffsetStore(String groupId) throws Exception {
                // initialize consumer side offset store, optionally
                // for example, create a database table to store offsets if not exists

                try (Connection connection = getConnection()) {
                    connection.createStatement().executeUpdate("create table if not exists kafkaoffset(group_id varchar, topic varchar, partition integer, offset integer, PRIMARY KEY (group_id, topic, partition))");
                    connection.commit();
                }
            }

            @Override
            public Map<TopicPartition, OffsetAndMetadata> loadOffsets(ConsumerGroupMetadata groupMetadata, Collection<TopicPartition> topicPartitions) throws Exception {
                // load consumer side offsets
                // for example, query the database table that has offsets

                Map<TopicPartition, OffsetAndMetadata> result = new HashMap<TopicPartition, OffsetAndMetadata>();
                try (Connection connection = getConnection()) {
                    PreparedStatement statement = connection.prepareStatement("select topic, partition, offset from kafkaoffset where group_id = ?");
                    statement.setString(1, groupMetadata.groupId());
                    ResultSet resultSet = statement.executeQuery();
                    while(resultSet.next()) {
                        TopicPartition topicPartition = new TopicPartition(resultSet.getString(1), resultSet.getInt(2));
                        if(topicPartitions.contains(topicPartition)) { // filter only to assigned partitions
                            result.put(topicPartition, new OffsetAndMetadata(resultSet.getLong(3)));
                        }
                    }
                }

                return result;
            }

            @Override
            public void saveOffsets(ConsumerGroupMetadata groupMetadata, Map<TopicPartition, OffsetAndMetadata> offsets, Connection transactionalObject) throws Exception {
                // save offsets on consumer side store
                // for example, upsert offsets in the database table

                PreparedStatement upsertStatement = transactionalObject.prepareStatement("insert into kafkaoffset(group_id, topic, partition, offset) values(?, ?, ?, ?) on conflict(group_id, topic, partition) do update set offset=?");
                for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
                    upsertStatement.setString(1, groupMetadata.groupId());
                    upsertStatement.setString(2, offset.getKey().topic());
                    upsertStatement.setInt(3, offset.getKey().partition());
                    upsertStatement.setLong(4, offset.getValue().offset());
                    upsertStatement.setLong(5, offset.getValue().offset());
                    upsertStatement.addBatch();
                }
                upsertStatement.executeBatch();
            }
        };

        // consume messages
        try (KafkaConsumer<Integer, Integer> kafkaConsumer = new KafkaConsumer<>(consumerProps)) {
            // call OffsetStoreContext.subscribe() with offsetStoreHandler, instead of kafkaConsumer.subscribe()
            OffsetStoreContext<Connection> offsetStoreContext = OffsetStoreContext.subscribe(offsetStoreProps, kafkaConsumer, Collections.singleton(TOPIC), consumerSideOffsetStoreHandler);

            while(true) {
                ConsumerRecords<Integer, Integer> records = kafkaConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<Integer, Integer> record : records) {
                    // use transactional object
                    // for example, create a JDBC connection and a transaction.
                    try (Connection connection = getConnection()) {
                        // do some business updates using your JDBC connection participating in the transaction
                        doSomething(record, connection);

                        // update consumer side offsets in the transaction before JDBC connection commit(), and optionally commit kafka side offsets
                        // this will call ConsumerSideOffsetStoreHandler.saveOffsets() with your JDBC connection participating in the transaction
                        offsetStoreContext.updateConsumerSideOffsets(record, connection);

                        // transaction commit
                        connection.commit();
                    }
                }
            }
        }
    }

    private static void doSomething(ConsumerRecord<Integer, Integer> record, Connection connection) {
        // do something
        System.out.println(record);
    }

    private static Connection getConnection() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Connection connection = DriverManager.getConnection("jdbc:sqlite::memory");
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return connection;
    }

    private static KafkaContainer startKafkaBroker() {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_CONTAINER));
        kafkaContainer.start();

        // create my-topic
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            try {
                adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC, 3, (short)1))).all().get();
            } catch (Exception e) {
                // void
            }
        }
        return kafkaContainer;
    }

    private static void loadDummydata() {
    }

    private static void startProducer(KafkaContainer kafkaContainer) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> producerProps = new HashMap<>();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
                KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerProps);

                for (int i = 0; i < Integer.MAX_VALUE; ++i) {
                    producer.send(new ProducerRecord<Integer, Integer>(TOPIC, i, i));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // void
                    }
                }
            }
        }).start();
    }
}
