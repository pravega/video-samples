package io.pravega.example.flinkprocessor;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class UTF8StringDeserializationSchema extends AbstractDeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) throws IOException {
        return new String(message, StandardCharsets.UTF_8);
    }
}