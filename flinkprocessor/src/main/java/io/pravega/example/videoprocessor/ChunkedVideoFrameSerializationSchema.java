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

import io.pravega.connectors.flink.serialization.PravegaSerializationSchema;
import io.pravega.example.common.ChunkedVideoFrame;

/**
 * Serializes ChunkedVideoFrame to JSON.
 */
public class ChunkedVideoFrameSerializationSchema extends PravegaSerializationSchema<ChunkedVideoFrame> {
    public ChunkedVideoFrameSerializationSchema() {
        super(new ChunkedVideoFrameSerializer());
    }
}
