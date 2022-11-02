/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.io.File;
import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleJdbcTestUtils {

    private static final Logger logger = LoggerFactory.getLogger(SimpleJdbcTestUtils.class);

    private static final String DBNAME = "testdb";
    public static final String JDBC_SQLITE_URI = "jdbc:sqlite:file:" + DBNAME;
    //public static final String JDBC_SQLITE_URI = "jdbc:sqlite:file::memory:?cache=shared";
    //public static final String JDBC_SQLITE_URI = "jdbc:sqlite:file::memory:?cache=shared&journal_mode=WAL";
    //public static final String JDBC_SQLITE_URI = "jdbc:sqlite:file:testdb?journal_mode=WAL&cache=shared";
    //public static final String JDBC_SQLITE_URI = "jdbc:sqlite:file:testdb?journal_mode=WAL";

    public static void initializeDatabase() throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        new File("testdb").delete();
    }

    static Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(JDBC_SQLITE_URI);
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        return connection;
    }

    public static void assertOffsets(EmbeddedKafkaBroker embeddedKafkaBroker, String consumerGroup, Map <TopicPartition, Long> expectedOffsets) throws Exception {
        try {
            Map <TopicPartition, Long> dbSideOffsets = getOffsetMap(consumerGroup);
            assertEquals(expectedOffsets, dbSideOffsets);

            Map<TopicPartition, Long> kafkaSideOffsets = TestUtils.getKafkaSideOffsets(embeddedKafkaBroker, consumerGroup, expectedOffsets.keySet());
            assertEquals(expectedOffsets, kafkaSideOffsets);

            Map<TopicPartition, Long> topicPartionLatestOffsets = TestUtils.getTopicPartionLatestOffsets(embeddedKafkaBroker, expectedOffsets.keySet());
            assertEquals(expectedOffsets, topicPartionLatestOffsets);

        } catch (Exception e) {
        throw new RuntimeException(e);
        }
    }

    public static Map <TopicPartition, Long> getOffsetMap(String group) {
        Map<TopicPartition, Long> result = new HashMap<>();
        try (Connection connection = createConnection()) {
            PreparedStatement statement = connection.prepareStatement("select topic, partition, offset from kafkaoffset where group_id = ?");
            statement.setString(1, group);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()) {
              result.put(new TopicPartition(resultSet.getString(1),resultSet.getInt(2)), resultSet.getLong(3));
            }

            connection.commit();
        } catch (Exception e) {
        throw new RuntimeException(e);
        }
        return result;
    }

    public static TestUtils.TransactionalObjectHandler<Connection> getTransactioncontextHandler() {
        return getTransactioncontextHandler((x, y)->{});
    }

    public static TestUtils.TransactionalObjectHandler<Connection> getTransactioncontextHandler(BiConsumer<Connection, KafkaConsumer> doSomething) {
        return new TestUtils.TransactionalObjectHandler<Connection>() {
        @Override
        public Connection getTransactionalObject() throws Exception {
            return createConnection();
        }

        @Override
        public void doSomething(Connection transactionalObject, KafkaConsumer kafkaConsumer) {
            doSomething.accept(transactionalObject, kafkaConsumer);
        }

        @Override
        public void commit(Connection transactionalObject) throws Exception {
            transactionalObject.commit();
        }
      };
    }


    public static class ConnectionConsumerSideOffsetStoreHandler implements ConsumerSideOffsetStoreHandler<Connection> {

        Map<TopicPartition, OffsetAndMetadata> initializeOffsets;

        public ConnectionConsumerSideOffsetStoreHandler(Map<TopicPartition, OffsetAndMetadata> initializeOffsets) {
            this.initializeOffsets = initializeOffsets;
        }

        @Override
        public void initializeOffsetStore(String groupId) throws Exception {
            try (Connection connection = SimpleJdbcTestUtils.createConnection()) {
                connection.createStatement().executeUpdate("create table if not exists kafkaoffset(group_id varchar, topic varchar, partition integer, offset integer, PRIMARY KEY (group_id, topic, partition))");

                try {
                    PreparedStatement insertStatement = connection.prepareStatement("insert into kafkaoffset values (?, ? , ?)");

                    for(Map.Entry<TopicPartition, OffsetAndMetadata> initializeOffset:initializeOffsets.entrySet()) {
                        insertStatement.setString(1, initializeOffset.getKey().topic());
                        insertStatement.setInt(2, initializeOffset.getKey().partition());
                        insertStatement.setLong(3, initializeOffset.getValue().offset());
                        insertStatement.addBatch();
                    }
                    insertStatement.executeBatch();
                } catch (Exception e) {
                    // void
                }

                ResultSet resultSet = connection.createStatement().executeQuery("select * from kafkaoffset");
                while(resultSet.next()) {
                    System.out.println("" + resultSet.getString(1)+ ":" + resultSet.getString(2)+ ":" + resultSet.getString(3));
                }

                connection.commit();
            }
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> loadOffsets(ConsumerGroupMetadata groupMetadata, Collection<TopicPartition> topicPartitions) throws Exception {
            Map<TopicPartition, OffsetAndMetadata> result = new HashMap<TopicPartition, OffsetAndMetadata>();
            try (Connection connection = SimpleJdbcTestUtils.createConnection()) {
                PreparedStatement statement = connection.prepareStatement("select topic, partition, offset from kafkaoffset where group_id = ?");
                statement.setString(1, groupMetadata.groupId());
                ResultSet resultSet = statement.executeQuery();
                while(resultSet.next()) {
                    TopicPartition topicPartition = new TopicPartition(resultSet.getString(1), resultSet.getInt(2));
                    if(topicPartitions.contains(topicPartition)) {
                        result.put(topicPartition, new OffsetAndMetadata(resultSet.getLong(3)));
                    }
                }

                connection.commit();
            }

            return result;
        }

        @Override
        public void saveOffsets(ConsumerGroupMetadata groupMetadata, Map<TopicPartition, OffsetAndMetadata> offsets, Connection transactionalObject) throws Exception {
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

            // todo test
            Statement statement = transactionalObject.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from kafkaoffset");
            while(resultSet.next()) {
                logger.info("" + resultSet.getString(1)+ ":" + resultSet.getString(2)+ ":" + resultSet.getString(3));
            }
        }
    }
}
