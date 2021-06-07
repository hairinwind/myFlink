package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionDeserializationSchema;
import my.flink.kafka.job.message.BankTransactionSerializationSchema;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static my.flink.kafka.job.Constants.BOOTSTRAP_SERVERS;

public class TransactionProcessTest {

    private static final String TOPIC_TRANSACTION = "alpha-bank-transactions-raw";
    public static final String TRANSACTION_RAW_COMPLETED = "alpha-bank-transactions-raw-completed";
    private static final String TOPIC_BACKUP = "alpha-bank-transactions-backup";

    public static void main(String[] args) throws Exception {
        Properties properties = kafkaProperties();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(Constants.FLINK_PARALLELISM);
//        streamEnv.enableCheckpointing(10000L);

        DataStream<BankTransaction> stream = streamEnv
                .addSource(new FlinkKafkaConsumer<>(TOPIC_TRANSACTION,
                        new BankTransactionDeserializationSchema(),
                        properties));

        AccountKeySelector accountKeySelector = new AccountKeySelector();
        SingleOutputStreamOperator<BankTransaction> afterDebitStream = stream.keyBy(accountKeySelector)
                .process(new BankTransactionProcessFunction());

        afterDebitStream.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS)
                .keyBy(accountKeySelector)
                .process(new BankTransactionProcessFunction())
                .addSink(kafkaProducer(TRANSACTION_RAW_COMPLETED));

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
                (record, timestamp) -> new ProducerRecord<byte[], byte[]>(topic, record.getFromAccount().getBytes(StandardCharsets.UTF_8), serializationSchema.serialize(record)),    // serialization schema
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
            ValueState<Double> balance = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("balance", Double.class)
            );
            if (balance.value() == null) {
                balance.update(0D);
            }
            System.out.println("threadId:" + Thread.currentThread().getId() + " : current balance "  + balance.value());
            if (bankTransaction.getStatus() == BankTransactionStatus.CREATED) {
                double newBalance = balance.value().doubleValue() - bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
            } else if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                double newBalance = balance.value().doubleValue() + bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            }
            System.out.println("threadId:" + Thread.currentThread().getId() + " : new balance "  + balance.value());
            collector.collect(bankTransaction);
        }

    }
}
