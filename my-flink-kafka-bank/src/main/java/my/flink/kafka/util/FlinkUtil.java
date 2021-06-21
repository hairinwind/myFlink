package my.flink.kafka.util;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import my.flink.kafka.job.message.JsonKafkaDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class FlinkUtil {

    public static StreamExecutionEnvironment getQueryableStreamEnv() {
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        return StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    public static DataStream<BankTransaction> getBankTransactionStream(StreamExecutionEnvironment env, String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "test001");

        DataStream<BankTransaction> stream = env
                .addSource(new FlinkKafkaConsumer<>(topic,
                        new BankTransactionDeserializationSchema(),
                        properties));
        return stream;
    }

    public static <T> DataStream<T> getKafkaStream(StreamExecutionEnvironment env, String topic, Class<T> type) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "test001");

        DataStream<T> stream = env
                .addSource(new FlinkKafkaConsumer<>(topic,
                        new JsonKafkaDeserializationSchema<T>(type),
                        properties));
        return stream;
    }

    public static <IN> FlinkKafkaProducer<IN> getKafkaProducer(String topic, KafkaSerializationSchema<IN> serializationSchema) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        FlinkKafkaProducer<IN> myProducer = new FlinkKafkaProducer<>(
                topic,                  // target topic
                serializationSchema,    // serialization schema
                properties,             // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return myProducer;
    }
}
