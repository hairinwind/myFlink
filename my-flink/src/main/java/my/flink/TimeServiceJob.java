package my.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class TimeServiceJob {

    public static void main(String[] args) throws Exception {
        String hostName;
        Integer port;
        if (args.length != 2){
            hostName = "localhost";
            port = 9000;
        } else {
            hostName = args[0];
            port = Integer.parseInt(args[1]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> textStream = env.socketTextStream(hostName, port);

        textStream.keyBy(word -> word).process(new MyTimeProcessFunction())
            .addSink(new PrintSinkFunction<>());

        env.execute("my time process");
    }

    private static class MyTimeProcessFunction extends KeyedProcessFunction<String, String, String> {
        private static final Logger logger = LoggerFactory.getLogger(MyTimeProcessFunction.class);

        private String str = "";

        @Override
        public void processElement(String s, Context context, Collector<String> collector) throws Exception {
            str = s;
            logger.info("... registerProcessingTimeTimer {} ", s);
            context.timerService().registerProcessingTimeTimer(Instant.now().plusSeconds(2L).toEpochMilli());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            logger.info("... onTimer {}", str); /* this shall 2 seconds after the register time */
            out.collect(str);
        }
    }

}
