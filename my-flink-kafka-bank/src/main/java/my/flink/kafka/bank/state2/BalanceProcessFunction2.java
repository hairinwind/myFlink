package my.flink.kafka.bank.state2;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * In this solution, for the tx which balance is not enough for debit, it is not sent to kafka retry topic
 * a ListState is created here: balanceNotEnoughTransactionState
 * if one transaction cannot debit because there is no enough balance, the transaction is added to the listState
 * it starts a timer registerProcessingTimeTimer with max_wait time
 * if there is a "credit" happens on this account, re-check bank transactions in the ListState
 * if the balance is enough, do the "debit" and then it is collected and moved to next stage.
 * if the balance is not enough, the tx is put back into the ListState and wait for the next "credit"
 * if the timer is triggered, it means the max_wait time passes, it is set to "CANCELLED_BALANCE_NOT_ENOUGH" and move to next stage
 *
 * This solution has better performance than "send back to kafka retry topic" (implemented in my.flink.kafka.bank.state.BalanceProcessFunction)
 */
public class BalanceProcessFunction2 extends KeyedProcessFunction<String, BankTransaction, BankTransaction> {
    private static final Logger logger = LoggerFactory.getLogger(BalanceProcessFunction2.class);
    private transient ValueState<Double> balance;
    private ListState<BankTransaction> balanceNotEnoughTransactionState;
    private static final long TEN_MINUTES = 600000L;
    private static final long MAX_WAIT = 10000L;
    private static final int MAX_RETRY_TIME = 5;
    /*
     * 500ms 5s 50s 500s ...
     * return milliseconds
     */
    private long getNextTriggerTime(int retryTime, long max) {
        long millis = (long) (500 * Math.pow(10, retryTime));
        logger.info("... wait {} seconds ", millis / 1000.0);
        millis = Math.min(millis, max);
        return Instant.now().plusMillis(millis).toEpochMilli();
    }

    private long getMaxWaitTime() {
        return Instant.now().plusMillis(MAX_WAIT).toEpochMilli();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>(
                        "balance", // the state name
                        TypeInformation.of(new TypeHint<Double>() {
                        })); // type information
        descriptor.setQueryable("balance");
        balance = getRuntimeContext().getState(descriptor);

        ListStateDescriptor<BankTransaction> balanceNotEnoughTransactionListDescriptor =
                new ListStateDescriptor<>("balanceNotEnoughTransaction", BankTransaction.class);
        balanceNotEnoughTransactionState = getRuntimeContext().getListState(balanceNotEnoughTransactionListDescriptor);
    }

    @Override
    public void processElement(BankTransaction bankTransaction, Context context, Collector<BankTransaction> collector) throws Exception {
        if (balance.value() == null) {
            balance.update(0D);
        }
        if (shallDebit(bankTransaction)) {
            double currentBalance = balance.value();
            double newBalance = currentBalance - bankTransaction.getAmount();
            if (isFromExternal(bankTransaction) || newBalance >= 0) {
                debit(bankTransaction, context, collector);
            } else {
                bankTransaction.setStatus(BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH);
                logger.info("... balance {} is not enough for {}", currentBalance, bankTransaction);
                long maxWaitTime = getMaxWaitTime();
                bankTransaction.setMaxWaitTime(maxWaitTime);
                balanceNotEnoughTransactionState.add(bankTransaction);
                logger.info("... wait to {} to retry {}", maxWaitTime, bankTransaction);
                context.timerService().registerProcessingTimeTimer(maxWaitTime);
            }
        } else if (shallCredit(bankTransaction)) {
            double currentBalance = balance.value();
            double newBalance = currentBalance + bankTransaction.getAmount();
            bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            balance.update(newBalance);
            logger.info("... change balance from {} to {} by {} ", currentBalance, newBalance, bankTransaction);
            collector.collect(bankTransaction);
            // after credit success... the balance is increased, check if any pending transaction can do debit
            processPendingBankTransactions(balanceNotEnoughTransactionState, context, collector);
        }

    }

    private void processPendingBankTransactions(ListState<BankTransaction> balanceNotEnoughTransactionState, Context context, Collector<BankTransaction> collector) throws Exception {
        Iterable<BankTransaction> bankTransactions = balanceNotEnoughTransactionState.get();
        List<BankTransaction> txLeft = new ArrayList<>();
        for(BankTransaction bankTransaction : bankTransactions) {
            double currentBalance = balance.value();
            if (bankTransaction.getAmount() <= currentBalance) {
                debit(bankTransaction, context, collector);
            } else {
                /* the balance is still not enough, add the bankTransaction back to the list and wait for the next credit happens */
                txLeft.add(bankTransaction);
            }
        }
        balanceNotEnoughTransactionState.update(txLeft);
    }

    private void debit(BankTransaction bankTransaction, Context context, Collector<BankTransaction> collector) throws java.io.IOException {
        double currentBalance = balance.value();
        double newBalance = currentBalance - bankTransaction.getAmount();
        bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
        balance.update(newBalance);
        logger.info("... change balance from {} to {} by {} ", currentBalance, newBalance, bankTransaction);
        collector.collect(bankTransaction);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<BankTransaction> out) throws Exception {
        // when onTimer happens, it means the balance is not enough when the waiting period passes
        logger.info(".....onTimer {}", timestamp);
        Iterable<BankTransaction> bankTransactions = balanceNotEnoughTransactionState.get();
        List<BankTransaction> txLeft = new ArrayList<>();
        for(BankTransaction bankTransaction : bankTransactions) {
            if (timestamp + 1000 >= bankTransaction.getMaxWaitTime()) { // give 1 second space to avoid timer is triggered a bit early
                bankTransaction.setStatus(BankTransactionStatus.CANCELLED_BALANCE_NOT_ENOUGH);
                out.collect(bankTransaction);
            } else {
                txLeft.add(bankTransaction);
            }
        }
        balanceNotEnoughTransactionState.update(txLeft);
        logger.info(".....onTimer {}", txLeft);
    }

    private boolean isFromExternal(BankTransaction bankTransaction) {
        return bankTransaction.getFromAccount().equalsIgnoreCase("external");
    }

    private boolean shallCredit(BankTransaction bankTransaction) {
        return bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS;
    }

    private boolean shallDebit(BankTransaction bankTransaction) {
        return bankTransaction.getStatus() == BankTransactionStatus.CREATED;
    }

}
