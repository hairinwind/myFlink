package my.flink.kafka.job;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class TransactionProcessStreamConnectJob {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessStreamConnectJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamEnv.setParallelism(3);

        DataStream<BankTransaction> stream = streamEnv.fromElements(
                new BankTransaction("100001", "100002", 1D),
                new BankTransaction("100002", "100003", 1D)
        );

        KeyedStream<BankTransaction, String> keyByFromAccountStream =
                stream.keyBy(bankTransaction -> bankTransaction.getFromAccount());

        KeyedStream<BankTransaction, String> keyByToAccountStream =
                stream.keyBy(bankTransaction -> bankTransaction.getToAccount());

        keyByFromAccountStream.connect(keyByToAccountStream)
                .flatMap(new ConnectFunction())
                .print("myOutput");

        streamEnv.execute("mytest to consume json messages");
    }

    private static class ConnectFunction extends RichCoFlatMapFunction<BankTransaction, BankTransaction, BankTransaction> {

        private ValueState<Double> balance;

        public void open(Configuration config) throws IOException {
            balance = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("balance", Double.class)
            );
        }

        @Override
        public void flatMap1(BankTransaction bankTransaction, Collector<BankTransaction> collector) throws Exception {
            logger.info("flatMap1 bankTransaction {}", Thread.currentThread().getId(), bankTransaction);
            double currentBalance = Optional.ofNullable(balance.value()).orElse(0D);
            logger.info("flatMap1 account: {} balance: {}", bankTransaction.getFromAccount(), currentBalance);
            double newBalance = currentBalance - bankTransaction.getAmount().doubleValue();
            balance.update(newBalance);
            bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
            logger.info("flatMap1 account: {} new balance: {}", bankTransaction.getFromAccount(), newBalance);
            collector.collect(bankTransaction);
        }

        @Override
        public void flatMap2(BankTransaction bankTransaction, Collector<BankTransaction> collector) throws Exception {
            logger.info("flatMap2 bankTransaction {}", bankTransaction);
            double currentBalance = Optional.ofNullable(balance.value()).orElse(0D);
            logger.info("flatMap2 account: {} balance: {}", bankTransaction.getToAccount(), currentBalance);
            double newBalance = currentBalance + bankTransaction.getAmount().doubleValue();
            balance.update(newBalance);
            bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            logger.info("flatMap2 account: {} new balance: {}", bankTransaction.getToAccount(), newBalance);
            collector.collect(bankTransaction);
        }
    }

}
