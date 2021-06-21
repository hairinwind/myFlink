package my.flink.kafka.bank.state;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import my.flink.kafka.job.message.BankTransactionStatus;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TxStateEvent {

    private String txId;
    private BankTransactionStatus status;

}
