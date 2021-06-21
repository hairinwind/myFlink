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

public class BalanceProcessFunction extends KeyedProcessFunction<String, BankTransaction, BankTransaction> {

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
            } else {
                bankTransaction.setStatus(BankTransactionStatus.RETRY_BALANCE_NOT_ENOUGH);
            }

        } else if (shallCredit(bankTransaction)) {
            double currentBalance = balance.value();
            double newBalance = currentBalance + bankTransaction.getAmount();
            bankTransaction.setStatus(BankTransactionStatus.FULFILLED);
            balance.update(newBalance);
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
