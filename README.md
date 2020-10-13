# Kafka Demo

Create a new topic in Zookeeper
Windows:
```shell
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic Lab3
```
Unix:
```shell
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic Lab3
```