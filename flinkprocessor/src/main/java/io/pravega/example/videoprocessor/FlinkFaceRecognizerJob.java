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

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.ExtendedEventPointer;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;


/**
 * This job reads and writes a video stream from Pravega and writes frame metadata to the console.
 */
public class FlinkFaceRecognizerJob extends AbstractJob {
    // Logger initialization
    private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        FlinkFaceRecognizerJob job = new FlinkFaceRecognizerJob(config);
        job.run();
    }

    public FlinkFaceRecognizerJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final String jobName = FlinkFaceRecognizerJob.class.getName();
            final StreamExecutionEnvironment env = initializeFlinkStreaming();
            final int mode = getConfig().getParams().getInt("mode", 2);
            log.info("mode={}", mode);
            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getOutputStreamConfig());
            final StreamCut startStreamCut = resolveStartStreamCut(getConfig().getInputStreamConfig());
            final StreamCut endStreamCut = resolveEndStreamCut(getConfig().getInputStreamConfig());
            log.info("startStreamCut={}", startStreamCut.asText());
            log.info("endStreamCut={}", endStreamCut.asText());

            // Read chunked video frames from Pravega.
            // Operator: input-source
            // Effective parallelism: min of # of segments, getReaderParallelism()
            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .setParallelism(getConfig().getReaderParallelism())
                    .uid("input-source")
                    .name("input-source");
            inChunkedVideoFrames
                    .printToErr()
                    .setParallelism(getConfig().getReaderParallelism())
                    .uid("input-source-print")
                    .name("input-source-print");

            // Calculate source event pointer.
            final DataStream<ChunkedVideoFrame> inChunkedVideoFramedWithSource = inChunkedVideoFrames.
                    map(videoFrame -> {
                        videoFrame.sourceEventPointer = new ExtendedEventPointer(videoFrame.eventReadMetadata, startStreamCut);
                        return videoFrame;
                    })
                    .uid("inChunkedVideoFramedWithSource")
                    .name("inChunkedVideoFramedWithSource");
            inChunkedVideoFramedWithSource.printToErr().uid("inChunkedVideoFramedWithSource-print").name("inChunkedVideoFramedWithSource-print");

            final DataStream<VideoFrame> outVideoFrames;

            if (mode == 0) {
                // BUG: THIS RESULTS IN OUT-OF-ORDER WRITES!

                // Unchunk (disabled).
                // Operator: ChunkedVideoFrameReassembler
                // Effective parallelism: default parallelism (implicit rebalance before operator) ???
                final DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramedWithSource
                        .map(VideoFrame::new)
                        .uid("ChunkedVideoFrameReassembler")
                        .name("ChunkedVideoFrameReassembler");

                // Identify objects with YOLOv3.
                // Effective parallelism: default parallelism
                final DataStream<VideoFrame> faceRecognizedFrames = inVideoFrames
                        .map(new FaceRecognizerMapFunction())
                        .uid("FaceRecognizedFrames")
                        .name("FaceRecognizedFrames");
                faceRecognizedFrames.printToErr().uid("FaceRecognizedFrames-print").name("FaceRecognizedFrames-print");
                outVideoFrames = faceRecognizedFrames;

            } else if (mode == 1) {
                // BUG: THIS RESULTS IN OUT-OF-ORDER WRITES!

                // Unchunk (disabled).
                // Operator: ChunkedVideoFrameReassembler
                // Effective parallelism: default parallelism (implicit rebalance before operator) ???
                final DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramedWithSource
                        .map(VideoFrame::new)
                        .uid("ChunkedVideoFrameReassembler")
                        .name("ChunkedVideoFrameReassembler");

                // Identify objects with YOLOv3.
                // Effective parallelism: default parallelism
                final DataStream<VideoFrame> objectDetectedFrames = inVideoFrames
                        .rebalance()
                        .map(new FaceRecognizerMapFunction())
                        .uid("objectDetectedFrames")
                        .name("objectDetectedFrames");
                objectDetectedFrames.printToErr().uid("objectDetectedFrames-print").name("objectDetectedFrames-print");
                outVideoFrames = objectDetectedFrames;

            } else if (mode == 2) {
                log.info("mode=2");
                // Assign timestamps and watermarks based on timestamp in each chunk.
                // Operator: assignTimestampsAndWatermarks
                // Effective parallelism: min of # of segments, getReaderParallelism()
                final DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFramedWithSource
                        .assignTimestampsAndWatermarks(
                                new BoundedOutOfOrdernessTimestampExtractor<ChunkedVideoFrame>(
                                        Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                    @Override
                                    public long extractTimestamp(ChunkedVideoFrame element) {
                                        return element.timestamp.getTime();
                                    }
                                })
                        .uid("assignTimestampsAndWatermarks")
                        .name("assignTimestampsAndWatermarks");

                // Unchunk (disabled).
                // Operator: ChunkedVideoFrameReassembler
                // Effective parallelism: default parallelism (implicit rebalance before operator) ???
                final DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramesWithTimestamps
                        .map(VideoFrame::new)
                        .uid("ChunkedVideoFrameReassembler")
                        .name("ChunkedVideoFrameReassembler");

                // For each camera and window, get the most recent frame.
                // This will emit at a maximum rate of the framesPerSec parameter.
                // Operator: lastVideoFramePerCamera
                // Effective parallelism: hash of camera
                final DataStream<VideoFrame> lastVideoFramePerCamera = inVideoFrames
                        .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                        .maxBy("timestamp")
                        .uid("lastVideoFramePerCamera")
                        .name("lastVideoFramePerCamera");

                DataStream<VideoFrame> faceRecognizedFrames = lastVideoFramePerCamera
                        .rebalance()
                        .map(new FaceRecognizerMapFunction())
                        .uid("faceRecognizedFrames")
                        .name("faceRecognizedFrames");
                faceRecognizedFrames.printToErr().uid("faceRecognizedFrames-print").name("faceRecognizedFrames-print");

                // Ensure ordering.
                // Effective parallelism: hash of camera
                outVideoFrames = faceRecognizedFrames
                        .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                        .maxBy("timestamp")
                        .uid("outVideoFrames")
                        .name("outVideoFrames");
                outVideoFrames.printToErr().uid("outVideoFrames-print").name("outVideoFrames-print");

                // Validate strictly increasing frame number. Throws an exception if events are out of order.
                if (false) {
                    final DataStream<VideoFrame> verifyOrderedVideoFrames = outVideoFrames
                            .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                            .process(new KeyedProcessFunction<Integer, VideoFrame, VideoFrame>() {
                                private ValueState<Long> lastFrameNumberState;

                                @Override
                                public void open(Configuration parameters) {
                                    lastFrameNumberState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastFrameNumber", Long.class));
                                }

                                @Override
                                public void processElement(VideoFrame value, Context ctx, Collector<VideoFrame> out) throws Exception {
                                    final Long lastFrameNumber = lastFrameNumberState.value();
                                    if (lastFrameNumber != null) {
                                        if (value.frameNumber <= lastFrameNumber) {
                                            log.error(MessageFormat.format(
                                                    "Unexpected frame number; current={0}, last={1}, camera={2}",
                                                    value.frameNumber, lastFrameNumber, value.camera));
                                        }
                                    }
                                    out.collect(value);
                                    lastFrameNumberState.update((long) value.frameNumber);
                                }
                            })
                            .uid("verifyOrderedVideoFrames")
                            .name("verifyOrderedVideoFrames");
                }
            } else {
                throw new IllegalArgumentException(MessageFormat.format("Unknown mode {0}", mode));
            }

            // Split output video frames into chunks of 8 MiB or less.
            // Effective parallelism: hash of camera
            final DataStream<ChunkedVideoFrame> chunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Write chunks to Pravega encoded as JSON.
            // Effective parallelism: hash of camera
            final FlinkPravegaWriter<ChunkedVideoFrame> writer = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            chunkedVideoFrames
                    .addSink(writer)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A map function that uses TensorFlow.
     * The TensorFlow Session cannot be serialized so it is declared transient and
     * initialized in open().
     */
    static class FaceRecognizerMapFunction extends RichMapFunction<VideoFrame, VideoFrame> {
        final private static Logger log = LoggerFactory.getLogger(FaceRecognizerMapFunction.class);
        private transient FaceRecognizer faceRecognizer;

        /**
         * The first execution takes 6 minutes on a V100.
         * We warmup in open() so that map() does not timeout.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            faceRecognizer = new FaceRecognizer();
            faceRecognizer.warmup();
        }

        @Override
        public void close() {
            faceRecognizer.close();
        }

        @Override
        public VideoFrame map(VideoFrame origFrame) throws Exception {
            log.info("map: BEGIN: camera={}, frameNumber={}", origFrame.camera, origFrame.frameNumber);
            final VideoFrame frame = faceRecognizer.recognizeFaces(origFrame);
            log.info("frame is shown here" + frame);
//            frame.data = result.getJpegBytes();
//            frame.recognitions = result.getRecognitions();
            log.info("map: END: camera={}, frameNumber={}", frame.camera, frame.frameNumber);
            return frame;
        }
    }
}
