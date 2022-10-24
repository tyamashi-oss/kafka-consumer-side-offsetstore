# ConsumerSideOffsetStoreHandler with JDBC connection Example
This example project use ConsumerSideOffsetStoreHandler with JDBC connection.
This example creates a relational database table as the consumer side offset store. 
And offsets are loaded and saved against that table. 

## Prerequisite
- Docker or Podman
  - This example use Testcontainer to run a Kafka cluster.
  - please see Testcontainer's prerequisites: https://testcontainers.org/

## Build and Run
To build and run this example, execute the following command.

```
$ mvn compile exec:java
```

When executed, the following like output will be logged. 
Press [Ctrl-c] to terminate the execution.
```
ConsumerRecord(topic = my-topic, partition = 1, leaderEpoch = 0, offset = 0, CreateTime = 1666616523220, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 0, value = 0)
ConsumerRecord(topic = my-topic, partition = 0, leaderEpoch = 0, offset = 0, CreateTime = 1666616524234, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 1, value = 1)
ConsumerRecord(topic = my-topic, partition = 2, leaderEpoch = 0, offset = 0, CreateTime = 1666616525234, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 2, value = 2)
ConsumerRecord(topic = my-topic, partition = 1, leaderEpoch = 0, offset = 1, CreateTime = 1666616526235, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 3, value = 3)
ConsumerRecord(topic = my-topic, partition = 1, leaderEpoch = 0, offset = 2, CreateTime = 1666616527235, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 4, value = 4)
ConsumerRecord(topic = my-topic, partition = 2, leaderEpoch = 0, offset = 1, CreateTime = 1666616528235, serialized key size = 4, serialized value size = 4, headers = RecordHeaders(headers = [], isReadOnly = false), key = 5, value = 5)
...
[Ctrl-c]
```