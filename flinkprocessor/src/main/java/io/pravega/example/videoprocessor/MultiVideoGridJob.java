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

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.VideoFrame;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Math.max;

/**
 * A Flink job that reads images from multiple cameras stored in a Pravega stream, combines them
 * into a square grid of images (like a security camera monitor), and writes the resulting
 * images to another Pravega stream.
 * Images are chunked into 512 KB chunks to allow for very large images.
 */
public class MultiVideoGridJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        MultiVideoGridJob job = new MultiVideoGridJob(config);
        job.run();
    }

    public MultiVideoGridJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob.class.getName();
            final StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getOutputStreamConfig());

            final StreamCut startStreamCut;
            if (getConfig().isStartAtTail()) {
                startStreamCut = getStreamInfo(getConfig().getInputStreamConfig().getStream()).getTailStreamCut();
            } else {
                startStreamCut = StreamCut.UNBOUNDED;
            }

            // Read chunked video frames from Pravega.
            // Operator: input-source
            // Effective parallelism: min of # of segments, getReaderParallelism()
            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .setParallelism(getConfig().getReaderParallelism())
                    .uid("input-source")
                    .name("input-source");
            inChunkedVideoFrames.printToErr();

            // Assign timestamps and watermarks based on timestamp in each chunk.
            // Operator: assignTimestampsAndWatermarks
            // Effective parallelism: min of # of segments, getReaderParallelism()
            final DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFrames
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
//            inChunkedVideoFramesWithTimestamps.printToErr().uid("inChunkedVideoFramesWithTimestamps-print").name("inChunkedVideoFramesWithTimestamps-print");

            // Unchunk (disabled).
            // Operator: ChunkedVideoFrameReassembler
            // Effective parallelism: default parallelism (implicit rebalance before operator)
            final DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramesWithTimestamps
                    .map(VideoFrame::new)
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");

            // For each camera and window, get the most recent frame.
            // Operator: lastVideoFramePerCamera
            // Effective parallelism: hash of camera
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final int imageWidth = getConfig().getImageWidth();
            final int imageHeight = getConfig().getImageHeight();
            final DataStream<VideoFrame> lastVideoFramePerCamera = inVideoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    // TODO: Use a sliding window.
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");

            // Resize all input images.
            // Operator: ImageResizer
            // Effective parallelism: default parallelism
            lastVideoFramePerCamera.printToErr();
            final DataStream<VideoFrame> resizedVideoFrames = lastVideoFramePerCamera
                    .rebalance()
                    .map(frame -> {
                        ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
                        frame.data = resizer.resize(frame.data);
                        frame.hash = null;
                        return frame;
                    })
                    .uid("ImageResizer")
                    .name("ImageResizer");
//            resizedVideoFrames.printToErr().uid("resizedVideoFrames-print").name("resizedVideoFrames-print");;

            // Aggregate resized images. This only adds images to a hash map. The actual work is done later to increase parallelism.
            // Operator: ImageAggregator
            // Effective parallelism: hash of monitor
            final DataStream<ImageAggregatorResult> aggResults = resizedVideoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> getMonitorFromCamera(value.camera))
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .process(new ProcessWindowFunction<VideoFrame, ImageAggregatorResult, Integer, TimeWindow>() {
                        private ValueState<Integer> frameNumberState;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            frameNumberState = getRuntimeContext().getState(new ValueStateDescriptor<>("frameNumber", Integer.class));
                        }

                        @Override
                        public void process(Integer camera, Context context, Iterable<VideoFrame> elements, Collector<ImageAggregatorResult> out) throws Exception {
                            final ImageAggregatorResult result = new ImageAggregatorResult();
                            elements.forEach(element -> {
                                 result.camera = camera;
                                 result.timestamp = new Timestamp(max(result.timestamp.getTime(), element.timestamp.getTime()));
                                 result.videoFrames.put(element.camera, element);
                            });
                            result.frameNumber = Optional.ofNullable(frameNumberState.value()).orElse(0) + 1;
                            frameNumberState.update(result.frameNumber);
                            out.collect(result);
                        }
                    })
                    .uid("ImageAggregator")
                    .name("ImageAggregator");

            final int ssrc = new Random().nextInt();

            // Build output images and encode as JPEG.
            // Effective parallelism: default parallelism
            final DataStream<VideoFrame> videoFrames = aggResults
                    .rebalance() // ensure that successive frames are round robin distributed to different subtasks
                    .map(aggResult -> new VideoFrame(aggResult.asVideoFrame(imageWidth, imageHeight, ssrc)))
                    .uid("asVideoFrame")
                    .name("asVideoFrame");

            // Ensure ordering.
            // Effective parallelism: hash of monitor
            final DataStream<VideoFrame> outVideoFrames = videoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("outVideoFrames")
                    .name("outVideoFrames");

            outVideoFrames.printToErr().uid("outVideoFrames-print").name("outVideoFrames-print");

            // Split output video frames into chunks of 8 MiB or less.
            // Effective parallelism: hash of monitor
            final DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Write chunks to Pravega encoded as JSON.
            // Effective parallelism: hash of monitor
            final FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            outChunkedVideoFrames
                    .addSink(flinkPravegaWriter)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: Make this configurable.
    static int camerasPerMonitor = 2;

    static int getMonitorFromCamera(final int camera) {
        return camera / camerasPerMonitor;
    }

    static int getPositionInMonitorFromCamera(final int camera) {
        return camera % camerasPerMonitor;
    }

    public static class ImageAggregatorAccum {
        public int camera;
        // Map from camera to last VideoFrame.
        public Map<Integer, VideoFrame> videoFrames = new HashMap<>();
        // Maximum timestamp from cameras.
        public Timestamp timestamp = new Timestamp(0);
        public int frameNumber;

        public ImageAggregatorAccum() {
        }

        @Override
        public String toString() {
            return "ImageAggregatorAccum{" +
                    "camera=" + camera +
                    ",timestamp=" + timestamp +
                    ",frameNumber=" + frameNumber +
                    '}';
        }
    }

    public static class ImageAggregatorResult extends ImageAggregatorAccum {
        public ImageAggregatorResult() {
        }

        public VideoFrame asVideoFrame(int imageWidth, int imageHeight, int ssrc) {
            log.info("asVideoFrame: BEGIN: frameNumber={}", frameNumber);
            final ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, videoFrames.size());
            videoFrames.forEach((camera, frame) -> builder.addImage(getPositionInMonitorFromCamera(camera), frame.data));
            final long t0 = System.currentTimeMillis();
            VideoFrame videoFrame = new VideoFrame();
            videoFrame.camera = camera;
            videoFrame.ssrc = ssrc + camera;
            videoFrame.timestamp = timestamp;
            videoFrame.frameNumber = frameNumber;
            videoFrame.data = builder.getOutputImageBytes("jpg");
            videoFrame.hash = videoFrame.calculateHash();
            videoFrame.tags = new HashMap<>();
            videoFrame.tags.put("numCameras", Integer.toString(videoFrames.size()));
            videoFrame.tags.put("cameras", videoFrames.keySet().toString());

//            long sleepTime = new Random().nextInt(1000);
//            // long sleepTime = (frameNumber % 2 == 0)
//            try {
//                Thread.sleep(sleepTime);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }

            log.info("asVideoFrame: END: frameNumber={}, TIME={}", frameNumber, System.currentTimeMillis() - t0);
            log.trace("asVideoFrame: videoFrame={}", videoFrame);
            return videoFrame;
        }
    }
}
