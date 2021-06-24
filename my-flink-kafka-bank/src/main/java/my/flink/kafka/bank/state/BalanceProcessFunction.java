package my.flink.kafka.bank.state;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.BankTransactionStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This solution send the bankTransaction back to a kafka retry topic
 * when a message is found in retry topic
 * if the status is DEBIT_SUCCESS, send it to TX_TOPIC to do credit
 * if the status is RETRY_BALANCE_NOT_ENOUGH, wait a specific period and send back to TX_TOPIC to retry
 * if the status is CANCELLED_BALANCE_NOT_ENOUGH, send the message to topic TRANSACTION_RAW_COMPLETED
 * if the status is FUL_FILLED, send the message to topic TRANSACTION_RAW_COMPLETED
 */
public class BalanceProcessFunction extends KeyedProcessFunction<String, BankTransaction, BankTransaction> {
    private static final Logger logger = LoggerFactory.getLogger(BalanceProcessFunction.class);
    private transient ValueState<Double> balance;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>(
                        "balance", // the state name
                        TypeInformation.of(new TypeHint<Double>() {
                        })); // type information
        descriptor.setQueryable("balance");
        balance = getRuntimeContext().getState(descriptor);
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
                bankTransaction.setStatus(BankTransactionStatus.DEBIT_SUCCESS);
                balance.update(newBalance);
                logger.info("... change balance from {} to {} by {} ", currentBalance, newBalance, bankTransaction);
            } else {
                bankTransaction.setStatus(BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH);
                logger.info("... balance {} is not enough for {}", currentBalance, bankTransaction);
            }
        } else if (shallCredit(bankTransaction)) {
            double currentBalance = balance.value();
            double newBalance = currentBalance + bankTransaction.getAmount();
            bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            balance.update(newBalance);
            logger.info("... change balance from {} to {} by {} ", currentBalance, newBalance, bankTransaction);
        }
        collector.collect(bankTransaction);
    }

    private boolean isFromExternal(BankTransaction bankTransaction) {
        return bankTransaction.getFromAccount().equalsIgnoreCase("external");
    }

    private boolean shallCredit(BankTransaction bankTransaction) {
        return bankTransaction.getStatus() == BankTransactionStatus.DEBIT_SUCCESS;
    }

    private boolean shallDebit(BankTransaction bankTransaction) {
        return bankTransaction.getStatus() == BankTransactionStatus.CREATED
                || bankTransaction.getStatus() == BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH;
    }

}
