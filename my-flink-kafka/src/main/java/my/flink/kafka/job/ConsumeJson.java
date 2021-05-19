package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class ConsumeJson {

    private static final String TOPIC = "alpha-bank-transactions-raw";

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

        //.print() is to output to the console, check the log by 'tail -f log/flink-*-taskexecutor-*.out'
        stream.map(record -> String.format("bankTransaction: send %s from %s to %s",
                record.getAmount(), record.getFromAccount(), record.getToAccount()))
                .print();
        streamEnv.execute("mytest to consume json messages");
    }
}
