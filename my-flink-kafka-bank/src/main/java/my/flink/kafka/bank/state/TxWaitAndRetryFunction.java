package my.flink.kafka.bank.state;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static my.flink.kafka.bank.state.AfterDebitJob.cancelledTransactions;
import static my.flink.kafka.bank.state.AfterDebitJob.retryTransactions;

public class TxWaitAndRetryFunction extends KeyedProcessFunction<String, BankTransaction, BankTransaction> {

    private static final Logger logger = LoggerFactory.getLogger(TxWaitAndRetryFunction.class);
    private static final long TEN_MINUTES = 600000L;
    private static final long MAX_WAIT = 10000L;
    private static final int MAX_RETRY_TIME = 5;

    /*
     * 500ms 5s 50s 500s ...
     * return milliseconds
     */
    private long getNextTriggerTime(int retryTime, long max) {
        long millis = (long) (500 * Math.pow(10, retryTime));
        millis = Math.min(millis, max);
        return Instant.now().plusMillis(millis).toEpochMilli();
    }

    // do i have to use state here?
    private ValueState<BankTransaction> bankTransactionState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<BankTransaction> stateDescriptor =
                new ValueStateDescriptor<>("bank transaction", BankTransaction.class);
        bankTransactionState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(BankTransaction bankTransaction, Context context, Collector<BankTransaction> collector) throws Exception {
        if (bankTransaction.getRetriedTimes() < MAX_RETRY_TIME) {
            bankTransactionState.update(bankTransaction);
            long nextTriggerTime = getNextTriggerTime(bankTransaction.getRetriedTimes(), MAX_WAIT);
            logger.info("... wait {} seconds to retry {}", nextTriggerTime / 1000, bankTransaction);
            context.timerService().registerProcessingTimeTimer(nextTriggerTime);
        } else {
            bankTransaction.setStatus(BankTransactionStatus.CANCELLED_BALANCE_NOT_ENOUGH);
            context.output(cancelledTransactions, bankTransaction);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<BankTransaction> out) throws Exception {
        BankTransaction bankTransaction = bankTransactionState.value();
        bankTransaction.setRetriedTimes(bankTransaction.getRetriedTimes() + 1);
        context.output(retryTransactions, bankTransaction);
        logger.info("... onTimer is triggered {}", bankTransaction);
        bankTransactionState.clear();
    }

}
