
## ref
this is the practice project following this document  
- https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/  
- https://www.baeldung.com/apache-flink
- https://www.baeldung.com/kafka-flink-data-pipeline

## create topic
```
bin/kafka-topics.sh --zookeeper localhost:2181 \
  --create --replication-factor 1 --partitions 1 \
  --topic test002
```
```
bin/kafka-topics.sh --zookeeper localhost:2181 \
  --create --replication-factor 1 --partitions 1 \
  --topic alpha-bank-transactions-backup
```

## start flink
If flink is not running, run ``` bin/start-cluster.sh ```to start it  
After start, visit http://localhost:8081/

## maven build to create the jar
```mvn clean package```

## deploy the task to flink
run this command
```
bin/flink run -c my.flink.kafka.job.WordUppercaseSink ~/myworkspace/myFlink/my-flink-kafka/target/my-flink-kafka-1.0-SNAPSHOT.jar
```

## Queryable State
[queryable state](QueryableState.md)





