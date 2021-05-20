package my.flink.kafka.job.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BankTransactionSerializationSchema implements SerializationSchema<BankTransaction> {

    Logger logger = LoggerFactory.getLogger(BankTransactionSerializationSchema.class);

    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .findAndAddModules() /* find the JavaTimeModule to handle the Instant in BankTransaction */
            .build();

    @Override
    public byte[] serialize(BankTransaction bankTransaction) {
        try {
            return objectMapper.writeValueAsString(bankTransaction).getBytes();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }

}
