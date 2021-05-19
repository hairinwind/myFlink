package my.flink.kafka.job.message;

public enum BankTransactionStatus {
    CREATED,
    RETRY_BALANCE_NOT_ENOUGH,
    DEBIT_SUCCESS,
//    CANCELLED_BALANCE_NOT_ENOUGH,
    FULFILLED;
}
