package my.flink.kafka.job.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SingleAccountBankTransaction {

    private String txId;
    private String account;
    private Double amount;
    private Instant instant;
    private BankTransactionStatus status;
    private int retriedTimes;

}
