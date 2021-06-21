
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

## steps
### transaction state machine
- a bank transaction state is started from "CREATED"
- doing the debit from the source account, if success, the state is changed to "DEBIT_SUCCESS"
- if balance is not enough for debit, the state is changed to "RETRY_BALANCE_NOT_ENOUGH"
- after retry, if the balance is enough, the state is changed from "RETRY_BALANCE_NOT_ENOUGH" to "DEBIT_SUCCESS" 
- if state is "DEBIT_SUCCESS", it can do credit to the target account.
- if success, the state is changed from "DEBIT_SUCEESS" to "FULFILLED"

```  
<kafka_dir>/bin/kafka-topics.sh --zookeeper 192.168.49.2:32181 \
--create \
--replication-factor 2 \
--partitions 3 \
--topic alpha-bank-transactions-state
```

### retry 
- the retry object 
- the target topic when retry is triggered  
- the time to retry it
A schedule service is watching on the topic. Once it is triggered, it gets all the records from the topic.  
It filters the records and only keeps those retryTime < now.
It sends the retry object to the target topic.
