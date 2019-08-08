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

/**
 * Allows a VideoFrame to be split into smaller chunks.
 * VideoFrame.data contains the chunk of data.
 */
public class ChunkedVideoFrame extends VideoFrame {
    // 0-based chunk index. The first chunk of each frame has chunkIndex 0.
    public short chunkIndex;
    // Number of chunks minus 1.
    public short finalChunkIndex;

    public ChunkedVideoFrame() {
    }

    public ChunkedVideoFrame(VideoFrame frame) {
        super(frame);
    }

    @Override
    public String toString() {
        return "ChunkedVideoFrame{" +
                super.toString() +
                ", chunkIndex=" + chunkIndex +
                ", finalChunkIndex=" + finalChunkIndex +
                '}';
    }
}
