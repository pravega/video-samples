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
            createStream(getConfig().getOutputStreamConfig());

            // Generate a stream of sequential frame numbers along with timestamps.
            DataStream<Tuple2<Integer,Long>> frameNumbers = env.fromCollection(
                    new FrameNumberIterator(getConfig().getFramesPerSec()),
                    TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>(){}))
                    .uid("frameNumbers")
                    .name("frameNumbers");

            // Generate a stream of video frames.
            int[] cameras = IntStream.range(0, getConfig().getNumCameras()).toArray();
            int ssrc = new Random().nextInt();
            int width = getConfig().getImageWidth();
            int height = width;
            DataStream<VideoFrame> videoFrames =
                    frameNumbers.flatMap(new FlatMapFunction<Tuple2<Integer,Long>, VideoFrame>() {
                        @Override
                        public void flatMap(Tuple2<Integer,Long> in, Collector<VideoFrame> out) {
                            for (int camera: cameras) {
                                VideoFrame frame = new VideoFrame();
                                frame.camera = camera;
                                frame.ssrc = ssrc + camera;
                                frame.timestamp = new Timestamp(in.f1);
                                frame.frameNumber = in.f0;
                                frame.data = new ImageGenerator(width, height).generate(frame.camera, frame.frameNumber);
                                frame.hash = frame.calculateHash();
                                out.collect(frame);
                            }
                        }
                    })
                    .uid("videoFrames")
                    .name("videoFrames");
            videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            // Split video frames into chunks of 1 MB or less. We must account for base-64 encoding, header fields, and JSON. Use 0.5 MB to be safe.
            DataStream<ChunkedVideoFrame> chunkedVideoFrames = videoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Drop some chunks (for testing).
            if (getConfig().isDropChunks()) {
                chunkedVideoFrames = chunkedVideoFrames.filter(f -> !(f.camera == 0 && (f.frameNumber + 1) % 10 == 0 && f.chunkIndex == f.finalChunkIndex));
            }

//            chunkedVideoFrames.printToErr().uid("chunkedVideoFrames-print").name("chunkedVideoFrames-print");

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            chunkedVideoFrames
                    .addSink(flinkPravegaWriter)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
