package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import my.flink.kafka.job.message.BankTransactionSerializationSchema;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class TransactionProcess {

    private static final String TOPIC = "alpha-bank-transactions-raw";
    public static final String TRANSACTION_RAW_COMPLETED = "alpha-bank-transactions-raw-completed";
    private static final String TOPIC_BACKUP = "alpha-bank-transactions-backup";

    public static void main(String[] args) throws Exception {
        Properties properties = kafkaProperties();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(Constants.FLINK_PARALLELISM);
//        streamEnv.enableCheckpointing(10000L);

        DataStream<BankTransaction> stream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(TOPIC,
                        new BankTransactionDeserializationSchema(),
                        properties));

//        SingleOutputStreamOperator<BankTransaction> process = stream
//                .keyBy(new AccountKeySelector())
//                .process(new BankTransactionProcessFunction());
//
//        process.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS)
//                .addSink(kafkaProducer(TOPIC));
//
//        process.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.FULFILLED)
//                .addSink(kafkaProducer(TRANSACTION_RAW_COMPLETED));

        stream.keyBy(new AccountKeySelector())
                .process(new BankTransactionProcessFunction())
                .addSink(kafkaProducer(TOPIC));

        //.print() is to output to the console, check the log by 'tail -f log/flink-*-taskexecutor-*.out'
//        stream.map(record -> String.format("bankTransaction: send %s from %s to %s and status is %s",
//                record.getAmount(), record.getFromAccount(), record.getToAccount(), record.getStatus()))
//                .print();

        // flink provided a sink to collect DataStream results for testing and debugging purposes
//        Iterator<BankTransaction> collect = DataStreamUtils.collect(process);
//        collect.forEachRemaining(record -> System.out.println("test output: " + record));

        streamEnv.execute("mytest to consume json messages");
    }

    private static Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "flink-bank");
        return properties;
    }

    private static FlinkKafkaProducer<BankTransaction> kafkaProducer(String topic) {
        BankTransactionSerializationSchema serializationSchema = new BankTransactionSerializationSchema();
        return new FlinkKafkaProducer<>(
                topic,                  // target topic
                (record, timestamp) -> new ProducerRecord<byte[], byte[]>(TOPIC_BACKUP, record.getFromAccount().getBytes(StandardCharsets.UTF_8), serializationSchema.serialize(record)),    // serialization schema
                kafkaProperties(),                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    private static class AccountKeySelector implements KeySelector<BankTransaction, String> {
        @Override
        public String getKey(BankTransaction bankTransaction) throws Exception {
            if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                return bankTransaction.getToAccount();
            }
            return bankTransaction.getFromAccount();
        }
    }

    private static class BankTransactionProcessFunction extends ProcessFunction<BankTransaction,BankTransaction> {

        @Override
        public void processElement(BankTransaction bankTransaction, Context context, Collector<BankTransaction> collector) throws Exception {
            System.out.println("threadId:" + Thread.currentThread().getId() + " : "  + bankTransaction);
            ValueState<Double> balance = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Double.class));
            if (balance.value() == null) {
                balance.update(0D);
            }
            if (bankTransaction.getStatus() == BankTransactionStatus.CREATED) {
                double newBalance = balance.value().doubleValue() - bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
            } else if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                double newBalance = balance.value().doubleValue() + bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            }
            collector.collect(bankTransaction);
        }

    }
}
