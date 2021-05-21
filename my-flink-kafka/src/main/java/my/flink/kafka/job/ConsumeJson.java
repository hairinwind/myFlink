package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import my.flink.kafka.job.message.BankTransactionSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class ConsumeJson {

    private static final String TOPIC = "alpha-bank-transactions-raw";
    private static final String TOPIC_BACKUP = "alpha-bank-transactions-backup";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "test001");

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStream<BankTransaction> stream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(TOPIC,
                        new BankTransactionDeserializationSchema(),
                        properties));

        //backup the message to topic
        BankTransactionSerializationSchema serializationSchema = new BankTransactionSerializationSchema();
        FlinkKafkaProducer<BankTransaction> backupProducer = new FlinkKafkaProducer<>(
                TOPIC_BACKUP,                  // target topic
                (record, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_BACKUP, record.getFromAccount().getBytes(StandardCharsets.UTF_8), serializationSchema.serialize(record)),    // serialization schema
                properties,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance
        stream.addSink(backupProducer);

        //.print() is to output to the console, check the log by 'tail -f log/flink-*-taskexecutor-*.out'
//        stream.map(record -> String.format("bankTransaction: send %s from %s to %s and status is %s",
//                record.getAmount(), record.getFromAccount(), record.getToAccount(), record.getStatus()))
//                .print();

        // flink provided a sink to collect DataStream results for testing and debugging purposes
        Iterator<BankTransaction> collect = DataStreamUtils.collect(stream);
        collect.forEachRemaining(record -> System.out.println("test output: " + record));

        streamEnv.execute("mytest to consume json messages");
    }
}
