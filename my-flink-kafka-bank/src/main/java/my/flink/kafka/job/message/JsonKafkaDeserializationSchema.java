package my.flink.kafka.job.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonKafkaDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .findAndAddModules() /* find the JavaTimeModule to handle the Instant in BankTransaction */
            .build();

    private final Class<T> typeParameterClass;

    public JsonKafkaDeserializationSchema(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, typeParameterClass);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(typeParameterClass);
    }

}
