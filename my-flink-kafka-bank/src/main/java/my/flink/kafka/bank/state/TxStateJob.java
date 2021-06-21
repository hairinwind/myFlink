package my.flink.kafka.bank.state;

import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.JsonKafkaSerializationSchema;
import my.flink.kafka.util.FlinkUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static my.flink.kafka.util.FlinkUtil.getBankTransactionStream;
import static my.flink.kafka.util.FlinkUtil.getQueryableStreamEnv;

public class TxStateJob {

    private static final String TX_TOPIC = "alpha-bank-transactions-raw";
    private static final String TX_STATE_TOPIC = "alpha-bank-transactions-state";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getQueryableStreamEnv();

        /* read tx from TX_TOPIC, then create TxStateEvent and sink it to TX_STATE_TOPIC */
        DataStream<BankTransaction> txStream = getBankTransactionStream(env, TX_TOPIC);
        FlinkKafkaProducer txStateKafkaProducer = FlinkUtil.getKafkaProducer(TX_STATE_TOPIC,
                new JsonKafkaSerializationSchema<TxStateEvent>(TX_STATE_TOPIC));
        txStream.keyBy(BankTransaction::getTxId)
                .map(bankTransaction -> new TxStateEvent(bankTransaction.getTxId(), bankTransaction.getStatus()))
                .addSink(txStateKafkaProducer);

        env.execute("create transaction state");
    }

}
