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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

import static java.lang.Math.min;

/**
 * A FlatMapFunction to create ChunkedVideoFrame instances from a VideoFrame.
 * The chunk size must account for base-64 encoding, header fields, and JSON.
 */
class VideoFrameChunker implements FlatMapFunction<VideoFrame, ChunkedVideoFrame> {
    private final int chunkSizeBytes;

    public VideoFrameChunker() {
        this.chunkSizeBytes = 512*1024;
    }

    public VideoFrameChunker(int chunkSizeBytes) {
        this.chunkSizeBytes = chunkSizeBytes;
    }

    @Override
    public void flatMap(VideoFrame in, Collector<ChunkedVideoFrame> out) {
        int numChunks = (in.data.length - 1) / chunkSizeBytes + 1;
        for (int chunkIndex = 0 ; chunkIndex < numChunks ; chunkIndex++) {
            ChunkedVideoFrame frame = new ChunkedVideoFrame(in);
            frame.data = Arrays.copyOfRange(frame.data, chunkIndex * chunkSizeBytes, min((chunkIndex + 1) * chunkSizeBytes, in.data.length));
            frame.chunkIndex = (short) chunkIndex;
            frame.finalChunkIndex = (short) (numChunks - 1);
        out.collect(frame);
        }
    }
}
