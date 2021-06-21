package my.flink.kafka.bank.state;

import my.flink.kafka.bank.generator.BankTransactionGenerator;
import my.flink.kafka.job.message.BankTransaction;
import my.flink.kafka.job.message.SingleAccountBankTransaction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static my.flink.kafka.util.FlinkUtil.getKafkaStream;
import static my.flink.kafka.util.FlinkUtil.getQueryableStreamEnv;

public class TxBroadcastStateJob {

    public static final String TX_BROADCAST_STATE = "TxBroadcastState";
    private static final String TX_STATE_TOPIC = "alpha-bank-transactions-state";
    private static final String TX_TOPIC = "alpha-bank-transactions-raw";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = getQueryableStreamEnv();

        /* read state from TX_STATE_TOPIC */
        DataStream<TxStateEvent> stateKafkaStream = getKafkaStream(env, TX_STATE_TOPIC, TxStateEvent.class);

        /* to broadcast state */
        MapStateDescriptor<String, TxStateEvent> stateDescriptor = new MapStateDescriptor<>(
                TX_BROADCAST_STATE,
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<TxStateEvent>() {}));

        BroadcastStream<TxStateEvent> txStateBroadcastStream = stateKafkaStream
                .broadcast(stateDescriptor);

        /* get tx and split them into singleAccountTx*/
        DataStream<BankTransaction> txStream = getKafkaStream(env, TX_TOPIC, BankTransaction.class);

        SingleOutputStreamOperator<SingleAccountBankTransaction> singleOutputStream = txStream.flatMap(new MapToSingleAccountTxFunction());

        singleOutputStream.keyBy(singleAccountBankTransaction -> singleAccountBankTransaction.getAccount())
                .connect(txStateBroadcastStream)
                .process(new SingleAccountTxProcessFunction())
                .addSink(new PrintSinkFunction<>());

        env.execute("broadcast tx state");
    }

    private static class MapToSingleAccountTxFunction extends RichFlatMapFunction<BankTransaction, SingleAccountBankTransaction> {
        @Override
        public void flatMap(BankTransaction bankTransaction, Collector<SingleAccountBankTransaction> collector) throws Exception {
            BankTransactionGenerator.toSingleAccountBankTransaction(bankTransaction)
                    .stream().forEach(singleAccountTx -> collector.collect(singleAccountTx));
        }
    }

    private static class SingleAccountTxProcessFunction extends KeyedBroadcastProcessFunction<String, SingleAccountBankTransaction, TxStateEvent, SingleAccountBankTransaction> {
        private static Logger logger = LoggerFactory.getLogger(SingleAccountTxProcessFunction.class);
        private MapStateDescriptor<String, TxStateEvent> stateDescriptor = new MapStateDescriptor<>(
                TX_BROADCAST_STATE,
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<TxStateEvent>() {}));

        @Override
        public void processElement(SingleAccountBankTransaction singleAccountBankTransaction,
                                   ReadOnlyContext readOnlyContext,
                                   Collector<SingleAccountBankTransaction> collector) throws Exception {
            Iterable<Map.Entry<String, TxStateEvent>> entries = readOnlyContext.getBroadcastState(stateDescriptor).immutableEntries();
            logger.info("processElement:" + singleAccountBankTransaction + " | " + entries);
        }

        @Override
        public void processBroadcastElement(TxStateEvent txStateEvent,
                                            Context context,
                                            Collector<SingleAccountBankTransaction> collector) throws Exception {
            logger.info("processBroadcastElement: " + txStateEvent);
            BroadcastState<String, TxStateEvent> broadcastState = context.getBroadcastState(stateDescriptor);
            logger.info("broadcastState hasNext? {}", broadcastState.immutableEntries().iterator().hasNext());
            for (Map.Entry<String, TxStateEvent> entry : broadcastState.immutableEntries()) {
                logger.info("... current state: {} | {}", entry.getKey(), entry.getValue());
            }
            broadcastState.put(txStateEvent.getTxId(), txStateEvent);
        }
    }

}
