# Java读取CSV文件摄入到kafka，使用avro序列化
- 注意：csv文件每个字段之间必须使用逗号分隔，每个逗号之间不能有空格，字段值如果有空格，那么字段值必须使用双引号引起来。
- kfk相关命令
```shell
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic topic_name
./bin/kafka-console-consumer.sh --topic topic_name --bootstrap-server localhost:9092 --from-beginning
./bin/kafka-topics.sh --list --zookeeper localhost:2181
./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic topic_name --time -1 --broker-list localhost:9092 --partitions 0
```
