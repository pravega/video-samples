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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.stream.StreamSupport;

/**
 * A ProcessWindowFunction that merges ChunkedVideoFrame instances to produce VideoFrame instances.
 */
public class ChunkedVideoFrameReassembler extends ProcessWindowFunction<ChunkedVideoFrame, VideoFrame, Tuple, TimeWindow> {
    private static Logger log = LoggerFactory.getLogger(ChunkedVideoFrameReassembler.class);

    @Override
    public void process(Tuple key, Context context, Iterable<ChunkedVideoFrame> elements, Collector<VideoFrame> out) throws ChunkSequenceException {
        // TODO: Do we need to handle out-of-order chunks?
        Iterator<ChunkedVideoFrame> it = elements.iterator();
        if (!it.hasNext()) {
            return;
        }
        ChunkedVideoFrame firstChunk = it.next();

        int totalSize = StreamSupport.stream(elements.spliterator(), false).mapToInt((e) -> e.data.length).sum();
        VideoFrame videoFrame = new VideoFrame();
        videoFrame.camera = firstChunk.camera;
        videoFrame.ssrc = firstChunk.ssrc;
        videoFrame.timestamp = firstChunk.timestamp;
        videoFrame.frameNumber = firstChunk.frameNumber;
        videoFrame.hash = firstChunk.hash;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        short expectedChunkIndex = 0;
        for (ChunkedVideoFrame chunk: elements) {
            if (chunk.finalChunkIndex != firstChunk.finalChunkIndex) {
                throw new ChunkSequenceException(MessageFormat.format(
                        "finalChunkIndex ({0}) does not match that of first chunk ({1})",
                        chunk.finalChunkIndex, firstChunk.finalChunkIndex));
            }
            if (chunk.chunkIndex < 0 || chunk.chunkIndex > firstChunk.finalChunkIndex) {
                throw new ChunkSequenceException(MessageFormat.format(
                        "chunkIndex ({0}) is not between 0 and finalChunkIndex ({1})",
                        chunk.chunkIndex, firstChunk.finalChunkIndex));
            }
            if (chunk.chunkIndex != expectedChunkIndex) {
                throw new ChunkSequenceException(MessageFormat.format(
                        "chunkIndex ({0}) does not match the expected value ({1})",
                        chunk.chunkIndex, expectedChunkIndex));
            }
            buf.put(chunk.data);
            expectedChunkIndex++;
        }
        if (expectedChunkIndex != firstChunk.finalChunkIndex + 1) {
            throw new ChunkSequenceException(MessageFormat.format(
                    "Number of chunks received ({0}) does not match expected value ({1})",
                    expectedChunkIndex, firstChunk.finalChunkIndex + 1));
        }
        buf.flip();
        videoFrame.data = buf.array();
        videoFrame.validateHash();
        out.collect(videoFrame);
    }
}
