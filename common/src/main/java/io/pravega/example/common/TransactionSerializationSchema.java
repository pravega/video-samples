package io.pravega.example.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class TransactionSerializationSchema implements SerializationSchema<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(Transaction element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize", e);
        }
    }
}
