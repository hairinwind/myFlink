package my.flink.kafka.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * This is to test connecting two stream and share the state
 */
public class StreamingConnectJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        DataStream<String> data = env
                .fromElements("IGNORE", "Flink", "DROP", "Forward")
                .keyBy(x -> x);

        control
                .connect(data)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String controlValue, Collector<String> out) throws Exception {
            System.out.println("flatMap1: " + controlValue);
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            System.out.println("flatMap2: " + dataValue);
            if (blocked.value() == null) {
                out.collect(dataValue);
            }
        }
    }
}
