package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class ChunkedVideoFrameSerializationSchema implements SerializationSchema<ChunkedVideoFrame> {

    private static final long serialVersionUID = 8414808687051531685L;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(ChunkedVideoFrame element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize", e);
        }
    }
}
