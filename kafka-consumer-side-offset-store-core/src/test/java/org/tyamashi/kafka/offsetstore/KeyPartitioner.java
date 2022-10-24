/*
 *     Copyright org.tyamashi authors.
 *     License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;

public class KeyPartitioner extends RoundRobinPartitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return (Integer) key;
    }
}
