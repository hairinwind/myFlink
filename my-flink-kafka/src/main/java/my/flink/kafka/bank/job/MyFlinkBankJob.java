package my.flink.kafka.bank.job;

import my.flink.bank.data.generator.BankTransactionGenerator;
import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import my.flink.kafka.job.message.SingleAccountBankTransaction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class MyFlinkBankJob {

    private static final String TOPIC = "alpha-bank-transactions-raw";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();

        DataStream<BankTransaction> txStream = getBankTransactionStream(env);

        txStream.print();

        SingleOutputStreamOperator<SingleAccountBankTransaction> singleOutputStream = txStream.flatMap(new MapToSingleAccountTxFunction());
        singleOutputStream.print();

        singleOutputStream.keyBy(singleAccountBankTransaction -> singleAccountBankTransaction.getAccount())
                .process(new SingleAccountTxProcessFunction());

//        System.out.println("jobId: " + env.getStreamGraph().getJobGraph().getJobID());

        env.execute("my flink bank balance");
    }

    // ##ENABLE_QUERYABLE_STATE_PROXY_SERVER
    private static StreamExecutionEnvironment getStreamEnv() {
        Configuration config = new Configuration();
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        return StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    private static DataStream<BankTransaction> getBankTransactionStream(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "test001");

        DataStream<BankTransaction> stream = env
                .addSource(new FlinkKafkaConsumer<>(TOPIC,
                        new BankTransactionDeserializationSchema(),
                        properties));
        return stream;
    }

    private static class MapToSingleAccountTxFunction extends RichFlatMapFunction<BankTransaction, SingleAccountBankTransaction> {
        @Override
        public void flatMap(BankTransaction bankTransaction, Collector<SingleAccountBankTransaction> collector) throws Exception {
            BankTransactionGenerator.toSingleAccountBankTransaction(bankTransaction)
                    .stream().forEach(singleAccountTx -> collector.collect(singleAccountTx));
        }
    }

    private static class SingleAccountTxProcessFunction extends KeyedProcessFunction<String, SingleAccountBankTransaction, SingleAccountBankTransaction> {

        private transient ValueState<Double> balance;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Double> descriptor =
                    new ValueStateDescriptor<>(
                            "balance", // the state name
                            TypeInformation.of(new TypeHint<Double>() {})); // type information
            descriptor.setQueryable("balance");
            balance = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(SingleAccountBankTransaction singleAccountBankTransaction, Context context, Collector<SingleAccountBankTransaction> collector) throws Exception {
            if (balance.value() == null) {
                balance.update(0D);
            }
            double newBalance = balance.value() + singleAccountBankTransaction.getAmount();
            //TODO: side output to record the balance is changed by which tx?
            balance.update(newBalance);
        }
    }
}
