package my.flink.kafka.job.message;

import java.time.Instant;
import java.util.UUID;

public class BankTransaction {

    private String txId = UUID.randomUUID().toString();
    private String fromAccount;
    private String toAccount;
    private Double amount;
    private Instant instant = Instant.now();
    private BankTransactionStatus status = BankTransactionStatus.CREATED;
    private int retriedTimes = 0;

    public BankTransaction() {
    }

    public BankTransaction(String fromAccount, String toAccount, Double amount) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
    }

    public String getTxId() {
        return txId;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public void setFromAccount(String fromAccount) {
        this.fromAccount = fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }

    public void setToAccount(String toAccount) {
        this.toAccount = toAccount;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Instant getInstant() {
        return instant;
    }

    public BankTransactionStatus getStatus() {
        return status;
    }

    public void setStatus(BankTransactionStatus status) {
        this.status = status;
    }

    public int getRetriedTimes() {
        return retriedTimes;
    }

    public void setRetriedTimes(int retriedTimes) {
        this.retriedTimes = retriedTimes;
    }

    @Override
    public String toString() {
        return "BankTransaction{" +
                "txId='" + txId + '\'' +
                ", status=" + status +
                ", fromAccount='" + fromAccount + '\'' +
                ", toAccount='" + toAccount + '\'' +
                ", amount=" + amount +
                ", instant=" + instant +
                ", retiedTimes =" + retriedTimes +
                '}';
    }
}
