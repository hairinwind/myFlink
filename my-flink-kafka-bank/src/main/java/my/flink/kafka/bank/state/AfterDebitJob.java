package my.flink.kafka.bank.state;

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

public class AfterDebitJob {

    private static final OutputTag<BankTransaction> debitSuccessTransactions =
            new OutputTag<BankTransaction>("debitSuccessTransactions") {};

    private static final OutputTag<BankTransaction> fulfilledTransactions =
            new OutputTag<BankTransaction>("fulfilledTransactions") {};

    private static final OutputTag<BankTransaction> balanceNotEnoughTransactions =
            new OutputTag<BankTransaction>("balanceNotEnoughTransactions") {};

    public static final OutputTag<BankTransaction> retryTransactions =
            new OutputTag<BankTransaction>("retryTransactions") {};

    public static final OutputTag<BankTransaction> cancelledTransactions =
            new OutputTag<BankTransaction>("cancelledTransactions") {};

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
        splittedAfterDebitStream.getSideOutput(fulfilledTransactions)
                .keyBy(BankTransaction::getTxId)
                .addSink(getBankTransactionKafkaProducer(TRANSACTION_RAW_COMPLETED));

        /* if balance is not enough, wait and then send back to TX_TOPIC to retry */
        SingleOutputStreamOperator<BankTransaction> afterWaitRetryStream = splittedAfterDebitStream.getSideOutput(balanceNotEnoughTransactions)
                .keyBy(BankTransaction::getTxId)
                .process(new TxWaitAndRetryFunction());
        afterWaitRetryStream.getSideOutput(retryTransactions)
                .addSink(getBankTransactionKafkaProducer(TX_TOPIC));
        afterWaitRetryStream.getSideOutput(cancelledTransactions)
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
            }
            if (bankTransaction.getStatus() == BankTransactionStatus.FULFILLED) {
                context.output(fulfilledTransactions, bankTransaction);
            }
            if (bankTransaction.getStatus() == BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH) {
                context.output(balanceNotEnoughTransactions, bankTransaction);
            }
        }
    }
}
