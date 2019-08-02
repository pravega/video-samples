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

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.text.MessageFormat;
import java.util.Iterator;

/**
 * A ProcessWindowFunction that concatenates ChunkedVideoFrame instances to produce VideoFrame instances.
 */
public class ChunkedVideoFrameReassembler extends ProcessWindowFunction<ChunkedVideoFrame, VideoFrame, Tuple, VideoFrameWindow> {
    private static Logger log = LoggerFactory.getLogger(ChunkedVideoFrameReassembler.class);

    private boolean failOnError = false;

    /**
     * @param failOnError If true, terminate the Flink task on any ignorable errors.
     *                    If false, such errors are logged and the corresponding VideoFrame is not emitted.
     */
    public ChunkedVideoFrameReassembler withFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public ChunkedVideoFrameReassembler withFailOnError() {
        return withFailOnError(true);
    }

    @Override
    public void process(Tuple key, Context context, Iterable<ChunkedVideoFrame> elements, Collector<VideoFrame> out) throws ChunkSequenceException {
        log.trace("process: window={}; elements={}", context.window(), elements);
        Iterator<ChunkedVideoFrame> it = elements.iterator();
        if (!it.hasNext()) {
            return;
        }
        ChunkedVideoFrame firstChunk = it.next();

        try {
            // Validate that chunks are in order and we have all of them.
            short expectedChunkIndex = 0;
            int totalSize = 0;
            for (ChunkedVideoFrame chunk : elements) {
                Preconditions.checkState(chunk.camera == context.window().getCamera());
                Preconditions.checkState(chunk.ssrc == context.window().getSsrc());
                Preconditions.checkState(chunk.timestamp.equals(context.window().getTimestamp()));
                if (chunk.finalChunkIndex != firstChunk.finalChunkIndex) {
                    throw new ChunkSequenceException(MessageFormat.format(
                            "finalChunkIndex ({0}) does not match that of first chunk ({1}); window={2}, elements={3}",
                            chunk.finalChunkIndex, firstChunk.finalChunkIndex, context.window(), elements));
                }
                if (chunk.chunkIndex < 0 || chunk.chunkIndex > firstChunk.finalChunkIndex) {
                    throw new ChunkSequenceException(MessageFormat.format(
                            "chunkIndex ({0}) is not between 0 and finalChunkIndex ({1}); window={2}, elements={3}",
                            chunk.chunkIndex, firstChunk.finalChunkIndex, context.window(), elements));
                }
                if (chunk.chunkIndex != expectedChunkIndex) {
                    throw new ChunkSequenceException(MessageFormat.format(
                            "chunkIndex ({0}) does not match the expected value ({1}); window={2}, elements={3}",
                            chunk.chunkIndex, expectedChunkIndex, context.window(), elements));
                }
                expectedChunkIndex++;
                totalSize += chunk.data.length;
            }
            if (expectedChunkIndex != firstChunk.finalChunkIndex + 1) {
                throw new ChunkSequenceException(MessageFormat.format(
                        "Number of chunks received ({0}) does not match expected value ({1}); window={2}, elements={3}",
                        expectedChunkIndex, firstChunk.finalChunkIndex + 1, context.window(), elements));
            }

            VideoFrame videoFrame = new VideoFrame(firstChunk);

            // Concatenate chunk data.
            ByteBuffer buf = ByteBuffer.allocate(totalSize);
            for (ChunkedVideoFrame chunk : elements) {
                buf.put(chunk.data);
            }
            buf.flip();
            videoFrame.data = buf.array();
            videoFrame.validateHash();
            out.collect(videoFrame);
        } catch (ChunkSequenceException | DigestException e) {
            if (failOnError) {
                throw new RuntimeException(e);
            }
            log.warn("Unable to reassemble frame:", e);
        }
    }
}
