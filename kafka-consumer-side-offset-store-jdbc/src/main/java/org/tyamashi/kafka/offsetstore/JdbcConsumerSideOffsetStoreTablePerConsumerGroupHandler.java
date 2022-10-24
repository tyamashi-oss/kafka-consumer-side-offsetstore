/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class JdbcConsumerSideOffsetStoreTablePerConsumerGroupHandler implements ConsumerSideOffsetStoreHandler<Connection> {

    private final Map<String, String> configuration;
    private final String offsetStoreTableName;
    private final String initializeOffsetStoreSql;
    private final String loadOffsetsSql;
    private final String saveOffsetsSql;

    public JdbcConsumerSideOffsetStoreTablePerConsumerGroupHandler(Map<String, String> configuration) {
        this.configuration = configuration;
        this.offsetStoreTableName = "kafkaoffsetstore";// TODO configuration
        this.initializeOffsetStoreSql = String.format("create table if not exists %s(topic varchar, partition integer, offset integer, PRIMARY KEY (topic, partition))", getOffsetStoreTableName());
        this.loadOffsetsSql = String.format("select topic, partition, offset from %s", getOffsetStoreTableName());
        this.saveOffsetsSql = String.format("insert into %s(topic, partition, offset) values(?, ?, ?) on conflict(topic, partition) do update set offset=?", getOffsetStoreTableName());
    }

    @Override
    public void initializeOffsetStore(String groupId) throws Exception {
        try (Connection connection = getConnection()) {
            connection.createStatement().executeUpdate(initializeOffsetStoreSql);
            connection.commit();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> loadOffsets(ConsumerGroupMetadata groupMetadata, Collection<TopicPartition> topicPartitions) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<TopicPartition, OffsetAndMetadata>();
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(loadOffsetsSql);
            while (resultSet.next()) {
                TopicPartition topicPartition = new TopicPartition(resultSet.getString(1), resultSet.getInt(2));
                if (topicPartitions.contains(topicPartition)) {
                    result.put(topicPartition, new OffsetAndMetadata(resultSet.getLong(3)));
                }
            }

            connection.commit();
        }

        return result;
    }

    @Override
    public void saveOffsets(ConsumerGroupMetadata groupMetadata, Map<TopicPartition, OffsetAndMetadata> offsets, Connection transactionalObject) throws Exception {
        System.out.println("saveOffset:" + offsets);

        PreparedStatement upsertStatement = transactionalObject.prepareStatement(saveOffsetsSql);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
            upsertStatement.setString(1, offset.getKey().topic());
            upsertStatement.setInt(2, offset.getKey().partition());
            upsertStatement.setLong(3, offset.getValue().offset());
            upsertStatement.setLong(4, offset.getValue().offset());
            upsertStatement.addBatch();
        }
        upsertStatement.executeBatch();
    }

    public String getOffsetStoreTableName() {
        return offsetStoreTableName;
    }

    public abstract Connection getConnection() throws SQLException;

}
