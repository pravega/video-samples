package io.pravega.example.tensorflow;

import io.pravega.example.common.Person;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class PersonSerializationSchema implements SerializationSchema<Person> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(Person element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize", e);
        }
    }
}
