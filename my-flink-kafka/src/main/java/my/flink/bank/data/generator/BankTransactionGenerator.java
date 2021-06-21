package my.flink.bank.data.generator;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.SingleAccountBankTransaction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BankTransactionGenerator {

    public static List<SingleAccountBankTransaction> get10BankTransactions() {
        return IntStream.range(1,11).boxed()
                .flatMap(index -> createSingleAccountBankTransactionByIndex(index))
                .collect(Collectors.toList());
    }

    private static Stream<SingleAccountBankTransaction> createSingleAccountBankTransactionByIndex(Integer index) {
        BankTransaction bankTx = createBankTransactionByIndex(index);
        return toSingleAccountBankTransaction(bankTx).stream();
    }

    public static List<SingleAccountBankTransaction> toSingleAccountBankTransaction(BankTransaction bankTx) {
        SingleAccountBankTransaction fromAccountTx = SingleAccountBankTransaction.builder()
                .txId(bankTx.getTxId())
                .account(bankTx.getFromAccount())
                .amount(bankTx.getAmount() * (-1))
                .instant(bankTx.getInstant())
                .status(bankTx.getStatus())
                .retriedTimes(bankTx.getRetriedTimes())
                .build();
        SingleAccountBankTransaction toAccountTx = SingleAccountBankTransaction.builder()
                .txId(bankTx.getTxId())
                .account(bankTx.getToAccount())
                .amount(bankTx.getAmount())
                .instant(bankTx.getInstant())
                .status(bankTx.getStatus())
                .retriedTimes(bankTx.getRetriedTimes())
                .build();
        return Arrays.asList(fromAccountTx, toAccountTx);
    }

    private static BankTransaction createBankTransactionByIndex(int index) {
        String fromAccount = String.valueOf(100000 + index);
        String toAccount = String.valueOf(100001 + index);
        return new BankTransaction(fromAccount, toAccount, 1D);
    }
}
