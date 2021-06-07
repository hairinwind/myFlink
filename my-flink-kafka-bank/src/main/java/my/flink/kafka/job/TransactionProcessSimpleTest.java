package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TransactionProcessSimpleTest {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessSimpleTest.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);

        DataStream<BankTransaction> stream = streamEnv.fromCollection(
                Arrays.asList(
                        new BankTransaction("100001", "100001", 1D)
                )
        );

        AccountKeySelector accountKeySelector = new AccountKeySelector();
        SingleOutputStreamOperator<BankTransaction> afterDebitStream = stream.keyBy(accountKeySelector)
                .process(new BankTransactionProcessFunction());

        afterDebitStream.filter(bankTransaction -> bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS)
                .keyBy(accountKeySelector)
                .process(new BankTransactionProcessFunction());

        streamEnv.execute("mytest to consume json messages");
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
            if (bankTransaction.getStatus() == BankTransactionStatus.CREATED) {
                logger.info("threadId: {} account: {} balance: {}", Thread.currentThread().getId(),
                            bankTransaction.getFromAccount(),
                            balance.value()
                        );
                double newBalance = balance.value().doubleValue() - bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
                logger.info("threadId: {} account: {} new balance: {}", Thread.currentThread().getId(),
                        bankTransaction.getFromAccount(),
                        newBalance
                );
            } else if (bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS) {
                logger.info("threadId: {} account: {} balance: {}", Thread.currentThread().getId(),
                        bankTransaction.getToAccount(),
                        balance.value()
                );
                double newBalance = balance.value().doubleValue() + bankTransaction.getAmount().doubleValue();
                balance.update(newBalance);
                bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
                logger.info("threadId: {} account: {} new balance: {}", Thread.currentThread().getId(),
                        bankTransaction.getFromAccount(),
                        newBalance
                );
            }
            collector.collect(bankTransaction);
        }

    }
}
