package my.flink.kafka.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WordUppercase {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.49.2:31090,192.168.49.2:31091,192.168.49.2:31092");
        properties.setProperty("group.id", "test001");

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStream<String> stream = streamEnv
                .addSource(new FlinkKafkaConsumer<>("test001", new SimpleStringSchema(), properties));

        //.print() is to output to the console, check the log by 'tail -f log/flink-*-taskexecutor-*.out'
        stream.map(String::toUpperCase).print();
        streamEnv.execute("word uppercase");
    }


}
