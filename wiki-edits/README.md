A Flink application project using Java and Gradle.

To package your job for submission to Flink, use: 'gradle shadowJar'. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

To run and test your application with an embedded instance of Flink use: 'gradle run'

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
$ ./bin/flink run ../wiki-edits-0.1.jar
```