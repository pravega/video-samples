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

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ChunkedVideoFrameDeserializationSchema extends AbstractDeserializationSchema<ChunkedVideoFrame> {

    private static final long serialVersionUID = 3505365087654130819L;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ChunkedVideoFrame deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, ChunkedVideoFrame.class);
    }
}
