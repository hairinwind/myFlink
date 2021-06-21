package my.flink.kafka.job.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonKafkaSerializationSchema<T> implements KafkaSerializationSchema<T> {

    Logger logger = LoggerFactory.getLogger(BankTransactionSerializationSchema.class);

    private String topic;

    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .findAndAddModules() /* find the JavaTimeModule to handle the Instant in BankTransaction */
            .build();

    public JsonKafkaSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T t, Long timestamp) {
        try {
            byte[] valueBytes = objectMapper.writeValueAsString(t).getBytes();
            return new ProducerRecord(topic, null, valueBytes);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new ProducerRecord(topic, null, new byte[0]);
    }
}
