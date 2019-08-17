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

import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.video.ChunkedVideoFrame;
import io.pravega.example.video.VideoFrame;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * This job simulates writing video from multiple cameras to Pravega.
 */
public class VideoDataGeneratorJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoDataGeneratorJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        VideoDataGeneratorJob job = new VideoDataGeneratorJob(config);
        job.run();
    }

    public VideoDataGeneratorJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = VideoDataGeneratorJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            if (getConfig().isWriteToPravega()) {
                createStream(getConfig().getOutputStreamConfig());
                }

            // Generate a stream of sequential frame numbers along with timestamps.
            DataStream<Tuple2<Integer,Long>> frameNumbers = env.fromCollection(
                    new FrameNumberIterator(getConfig().getFramesPerSec()),
                    TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>(){}))
                    .uid("frameNumbers")
                    .name("frameNumbers");

            // Generate a stream of empty video frames.
            int[] cameras = IntStream.range(0, getConfig().getNumCameras()).toArray();
            int ssrc = new Random().nextInt();
            DataStream<VideoFrame> emptyVideoFrames =
                    frameNumbers.flatMap(new FlatMapFunction<Tuple2<Integer,Long>, VideoFrame>() {
                        @Override
                        public void flatMap(Tuple2<Integer,Long> in, Collector<VideoFrame> out) {
                            for (int camera: cameras) {
                                VideoFrame frame = new VideoFrame();
                                frame.camera = camera;
                                frame.ssrc = ssrc + camera;
                                frame.timestamp = new Timestamp(in.f1);
                                frame.frameNumber = in.f0;
                                out.collect(frame);
                            }
                        }
                    })
                    .setParallelism(1)
                    .uid("emptyVideoFrames")
                    .name("emptyVideoFrames");
            emptyVideoFrames.printToErr().uid("emptyVideoFrames-print").name("emptyVideoFrames-print");

            // Generate images in parallel.
            final int width = getConfig().getImageWidth();
            final int height = width;
            final boolean isUseCachedFrame = true;
            byte[] cachedFrameData;
            byte[] cachedFrameHash;
            if (isUseCachedFrame) {
                VideoFrame cachedFrame = new VideoFrame();
                cachedFrame.data = new ImageGenerator(width, height).generate(0, 0);
                cachedFrameData = cachedFrame.data;
                cachedFrameHash = cachedFrame.calculateHash();
            }
            DataStream<VideoFrame> videoFrames = emptyVideoFrames
                    .keyBy("camera")
                    .map((frame) -> {
                        if (isUseCachedFrame) {
                            frame.data = cachedFrameData;
                            frame.hash = cachedFrameHash;
                        } else {
                            frame.data = new ImageGenerator(width, height).generate(frame.camera, frame.frameNumber);
                            frame.hash = frame.calculateHash();
                        }
                        return frame;
                    })
                    .uid("videoFrames")
                    .name("videoFrames");
//            videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            // Split video frames into chunks of 1 MB or less. We must account for base-64 encoding, header fields, and JSON. Use 0.5 MB to be safe.
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = videoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Drop some chunks (for testing).
            if (getConfig().isDropChunks()) {
                chunkedVideoFrames = chunkedVideoFrames.filter(f -> !(f.camera == 0 && (f.frameNumber + 1) % 10 == 0 && f.chunkIndex == f.finalChunkIndex));
            }

            // Print to screen a small subset.
            chunkedVideoFrames
                    .filter(f -> f.camera == 0 && f.frameNumber % 10 == 0)
                    .printToErr().uid("chunkedVideoFrames-print").name("chunkedVideoFrames-print");

            // Write chunks to Pravega encoded as JSON.
            if (getConfig().isWriteToPravega()) {
                FlinkPravegaWriter<ChunkedVideoFrame> sink = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                        .withPravegaConfig(getConfig().getPravegaConfig())
                        .forStream(getConfig().getOutputStreamConfig().getStream())
                        .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                        .withEventRouter(frame -> String.format("%d", frame.camera))
                        .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                        .build();
                chunkedVideoFrames
                        .addSink(sink)
                        .uid("output-sink")
                        .name("output-sink");
            }

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
