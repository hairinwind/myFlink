package my.flink.kafka.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class WordUppercaseSink {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.49.2:31090,192.168.49.2:31091,192.168.49.2:31092");
        properties.setProperty("group.id", "test_word_uppercase_sink");

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStream<String> stream = streamEnv
                .addSource(new FlinkKafkaConsumer<>("test001", new SimpleStringSchema(), properties));

        //producer
        String TOPIC_OUT = "test002";
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                TOPIC_OUT,                  // target topic
                (record, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_OUT, record.getBytes(), record.getBytes()),    // serialization schema
                properties,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        stream.map(String::toUpperCase).addSink(myProducer);

        streamEnv.execute("word uppercase sink");
    }


}
