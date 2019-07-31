package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ChunkedVideoFrameDeserializationSchema extends AbstractDeserializationSchema<ChunkedVideoFrame> {

    private static final long serialVersionUID = 3505365087654130819L;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ChunkedVideoFrame deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, ChunkedVideoFrame.class);
    }
}
