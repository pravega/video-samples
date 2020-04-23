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

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.EventReadMetadata;

/**
 * Deserializes ChunkedVideoFrame from JSON.
 */
public class ChunkedVideoFrameDeserializationSchema extends PravegaDeserializationSchema<ChunkedVideoFrame> {
    public ChunkedVideoFrameDeserializationSchema() {
        super(ChunkedVideoFrame.class, new ChunkedVideoFrameSerializer());
    }

    @Override
    public ChunkedVideoFrame extractEvent(EventRead<ChunkedVideoFrame> eventRead) {
        final ChunkedVideoFrame event = eventRead.getEvent();
        if (event != null) {
            event.eventReadMetadata = new EventReadMetadata(eventRead);
        }
        return event;
    }
}
