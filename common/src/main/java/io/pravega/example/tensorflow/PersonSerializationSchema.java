package io.pravega.example.tensorflow;
//
//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.databind.SerializerProvider;
//import com.fasterxml.jackson.databind.ser.std.StdSerializer;
//
//import java.io.IOException;
//
//public class PersonSerializer extends StdSerializer<Person> {
//
//    public PersonSerializer(Class<Person> t) {
//        super(t);
//    }
//
//    @Override
//    public void serialize(Person value, JsonGenerator gen, SerializerProvider provider) throws IOException {
//        gen.writeStartObject();
//        gen.writeObjectField("name", value.name);
//        gen.writeObjectField("embedding", value.embedding);
//        gen.writeEndObject();
//    }
//}

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
