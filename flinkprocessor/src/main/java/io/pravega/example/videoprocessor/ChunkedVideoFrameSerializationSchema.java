/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serializes ChunkedVideoFrame to JSON.
 */
public class ChunkedVideoFrameSerializationSchema implements SerializationSchema<ChunkedVideoFrame> {
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
