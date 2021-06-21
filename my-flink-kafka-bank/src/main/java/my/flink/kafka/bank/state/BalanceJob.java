package my.flink.kafka.bank.state;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import my.flink.kafka.job.message.JsonKafkaSerializationSchema;
import my.flink.kafka.util.FlinkUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static my.flink.kafka.bank.state.Constants.TX_RETRY_TOPIC;
import static my.flink.kafka.bank.state.Constants.TX_TOPIC;
import static my.flink.kafka.util.FlinkUtil.getKafkaStream;
import static my.flink.kafka.util.FlinkUtil.getQueryableStreamEnv;

public class BalanceJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getQueryableStreamEnv();

        DataStream<BankTransaction> txStream = getKafkaStream(env, TX_TOPIC, BankTransaction.class);
        FlinkKafkaProducer retryKafkaProducer = FlinkUtil.getKafkaProducer(TX_RETRY_TOPIC,
                new JsonKafkaSerializationSchema<BankTransaction>(TX_RETRY_TOPIC));
        txStream.keyBy(new TxKeySelectorFunction())
                .process(new BalanceProcessFunction())
                .addSink(retryKafkaProducer);  //to RETRY topic

        env.execute("transaction to balance");

    }

    private static class TxKeySelectorFunction implements KeySelector<BankTransaction, String> {
        @Override
        public String getKey(BankTransaction bankTransaction) throws Exception {
            if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                return bankTransaction.getToAccount();
            }
            return bankTransaction.getFromAccount();
        }
    }

}
