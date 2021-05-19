package my.flink.kafka.job.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class BankTransactionDeserializationSchema implements DeserializationSchema<BankTransaction> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .findAndAddModules()
            .build();

    @Override
    public BankTransaction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, BankTransaction.class);
    }

    @Override
    public boolean isEndOfStream(BankTransaction bankTransaction) {
        return false;
    }

    @Override
    public TypeInformation<BankTransaction> getProducedType() {
        return TypeInformation.of(BankTransaction.class);
    }
}
