# 官方文档示例wiki-edits

### Build libs
```bash
$ ./gradlew clean wiki-edits:shadowJar
```

## Kafka
```bash
# 创建wiki-result topic
$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wiki-result
$ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
wiki-result

# 订阅wiki-result topic
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wiki-result
(Sims2aholic8,88)
(RoyalSnowbird,44)
(Lugnuts,-20)
(Horst Hof,-14)
(Pfold,344)
```

## Run
```bash
$ ./bin/flink run {lib-path}/wiki-edits-0.1-all.jar
```