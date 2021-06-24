package my.flink.kafka.bank.state2;

import my.flink.kafka.bank.state.TxWaitAndRetryFunction;
import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import my.flink.kafka.job.message.JsonKafkaSerializationSchema;
import my.flink.kafka.util.FlinkUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static my.flink.kafka.bank.state.Constants.TRANSACTION_RAW_COMPLETED;
import static my.flink.kafka.bank.state.Constants.TX_RETRY_TOPIC;
import static my.flink.kafka.bank.state.Constants.TX_TOPIC;
import static my.flink.kafka.util.FlinkUtil.getKafkaStream;

public class AfterDebitJob2 {

    private static final OutputTag<BankTransaction> debitSuccessTransactions =
            new OutputTag<BankTransaction>("debitSuccessTransactions") {};

    private static final OutputTag<BankTransaction> completedTransactions =
            new OutputTag<BankTransaction>("completedTransactions") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<BankTransaction> afterDebitStream = getKafkaStream(env, TX_RETRY_TOPIC, BankTransaction.class);
        SingleOutputStreamOperator<BankTransaction> splittedAfterDebitStream = afterDebitStream.process(new AfterDebitSplitFunction());

        /* if it is debit success, send to TX_TOPIC and do the credit */
        splittedAfterDebitStream.getSideOutput(debitSuccessTransactions)
                .keyBy(BankTransaction::getTxId) //TODO is there benefit to do keyBy here?
                .addSink(getBankTransactionKafkaProducer(TX_TOPIC));

        /* if it is FULFILLED, send to TX_FULFILLED */
        splittedAfterDebitStream.getSideOutput(completedTransactions)
                .keyBy(BankTransaction::getTxId)
                .addSink(getBankTransactionKafkaProducer(TRANSACTION_RAW_COMPLETED));

        env.execute("transaction to balance");
    }

    private static SinkFunction<BankTransaction> getBankTransactionKafkaProducer(String topic) {
        return FlinkUtil.getKafkaProducer(topic,
                new JsonKafkaSerializationSchema<BankTransaction>(topic));
    }

    private static class AfterDebitSplitFunction extends ProcessFunction<BankTransaction, BankTransaction> {

        @Override
        public void processElement(BankTransaction bankTransaction, Context context, Collector<BankTransaction> collector) throws Exception {
            if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                context.output(debitSuccessTransactions, bankTransaction);
            } else {
                context.output(completedTransactions, bankTransaction);
            }
        }
    }
}
