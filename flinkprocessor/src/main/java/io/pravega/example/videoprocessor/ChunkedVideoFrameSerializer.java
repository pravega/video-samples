/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.videoprocessor;

import io.pravega.client.stream.Serializer;
import io.pravega.example.common.ChunkedVideoFrame;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Pravega Serializer for ChunkedVideoFrame.
 * Uses JSON.
 */
public class ChunkedVideoFrameSerializer implements Serializer<ChunkedVideoFrame>, Serializable {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ChunkedVideoFrame deserialize(ByteBuffer serializedValue) {
        final ByteArrayInputStream bin = new ByteArrayInputStream(
                serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        try {
            return mapper.readValue(bin, ChunkedVideoFrame.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer serialize(ChunkedVideoFrame element) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(element));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
