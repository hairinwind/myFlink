package my.flink.kafka.bank.state;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import my.flink.kafka.job.message.JsonKafkaSerializationSchema;
import my.flink.kafka.util.FlinkUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static my.flink.kafka.bank.state.Constants.TRANSACTION_RAW_COMPLETED;
import static my.flink.kafka.bank.state.Constants.TX_RETRY_TOPIC;
import static my.flink.kafka.bank.state.Constants.TX_TOPIC;
import static my.flink.kafka.util.FlinkUtil.getKafkaStream;

public class AfterDebitJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<BankTransaction> afterDebitStream = getKafkaStream(env, TX_RETRY_TOPIC, BankTransaction.class);
        FlinkKafkaProducer txKafkaProducer = FlinkUtil.getKafkaProducer(TX_TOPIC,
                new JsonKafkaSerializationSchema<BankTransaction>(TX_TOPIC));

        // side output ???
        afterDebitStream.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS)
                .addSink(txKafkaProducer); // if it is DEBIT_SUCCESS, send to TX_TOPIC and do the credit

        FlinkKafkaProducer txFulFilledKafkaProducer = FlinkUtil.getKafkaProducer(TRANSACTION_RAW_COMPLETED,
                new JsonKafkaSerializationSchema<BankTransaction>(TRANSACTION_RAW_COMPLETED));
        afterDebitStream.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.FULFILLED)
                .addSink(txFulFilledKafkaProducer); // if it is FULFILLED, send to TX_FULFILLED

        //if balance is not enough, wait and retry
//        afterDebitStream.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH)
//                .addSink(txWaitAndRetry); // if it is FULFILLED, send to TX_TOPIC and do the credit

        env.execute("transaction to balance");
    }

}
