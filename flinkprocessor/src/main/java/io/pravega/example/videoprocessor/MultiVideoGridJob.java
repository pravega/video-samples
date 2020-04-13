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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * A Flink job that reads images from multiple cameras stored in a Pravega stream, combines them
 * into a NxM grid of images (like a security camera monitor), and writes the resulting
 * images to another Pravega stream.
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
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final int numColumns = getConfig().getParams().getInt("numColumns", 2);
            final int numRows = getConfig().getParams().getInt("numRows", 2);
            final int camerasPerMonitor = getConfig().getParams().getInt("camerasPerMonitor", numColumns * numRows);
            final int imageWidth = getConfig().getImageWidth() / numColumns;
            final int imageHeight = getConfig().getImageHeight() / numRows;
            final long slidingWindowSizeMs = getConfig().getParams().getLong("slidingWindowSizeMs", 10000);
            final int ssrc = new Random().nextInt();
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
            inChunkedVideoFrames.printToErr().uid("input-source-print").name("input-source-print");

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

            // Unchunk (disabled).
            // Operator: ChunkedVideoFrameReassembler
            // Effective parallelism: default parallelism (implicit rebalance before operator)
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
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> getMonitorFromCamera(value.camera, camerasPerMonitor))
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    // TODO: SlidingEventTimeWindows below does not correctly. It stops emitting records after several seconds.
//                    .window(SlidingEventTimeWindows.of(Time.milliseconds(slidingWindowSizeMs), Time.milliseconds(periodMs)))
                    .process(new ProcessWindowFunction<VideoFrame, ImageAggregatorResult, Integer, TimeWindow>() {
                        private ValueState<Integer> frameNumberState;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            frameNumberState = getRuntimeContext().getState(new ValueStateDescriptor<>("frameNumber", Integer.class));
                        }

                        @Override
                        public void process(Integer monitor, Context context, Iterable<VideoFrame> elements, Collector<ImageAggregatorResult> out) throws Exception {
                            final ImageAggregatorResult result = new ImageAggregatorResult();
                            result.monitor = monitor;
                            result.ssrc = ssrc + monitor;
                            result.frameNumber = Optional.ofNullable(frameNumberState.value()).orElse(0) + 1;
                            result.imageWidth = imageWidth;
                            result.imageHeight = imageHeight;
                            result.numColumns = numColumns;
                            result.numRows = numRows;
                            elements.forEach(element -> {
                                 result.timestamp = new Timestamp(max(result.timestamp.getTime(), element.timestamp.getTime()));
                                 final int position = getPositionInMonitorFromCamera(element.camera, camerasPerMonitor);
                                 result.videoFrames.put(position, element);
                            });
                            frameNumberState.update(result.frameNumber);
                            out.collect(result);
                        }
                    })
                    .uid("ImageAggregator")
                    .name("ImageAggregator");

            // Build output images and encode as JPEG.
            // Effective parallelism: default parallelism
            final DataStream<VideoFrame> videoFrames = aggResults
                    .rebalance() // ensure that successive frames are round robin distributed to different subtasks
                    .map(aggResult -> new VideoFrame(aggResult.encodeVideoFrame()))
                    .uid("encodeVideoFrame")
                    .name("encodeVideoFrame");

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

    private static int getMonitorFromCamera(final int camera, final int camerasPerMonitor) {
        return camera / camerasPerMonitor;
    }

    private static int getPositionInMonitorFromCamera(final int camera, final int camerasPerMonitor) {
        return camera % camerasPerMonitor;
    }

    public static class ImageAggregatorResult {
        // ID for this monitor
        public int monitor;
        public int ssrc;
        // Map from position to VideoFrame.
        public Map<Integer, VideoFrame> videoFrames = new HashMap<>();
        // Maximum timestamp from cameras.
        public Timestamp timestamp = new Timestamp(0);
        public int frameNumber;
        // Width of each input image
        public int imageWidth;
        // Height of each input image
        public int imageHeight;
        // Number of columns in grid
        public int numColumns;
        // Number of rows in grid
        public int numRows;

        public ImageAggregatorResult() {
        }

        @Override
        public String toString() {
            return "ImageAggregatorResult{" +
                    "monitor=" + monitor +
                    ", ssrc=" + ssrc +
                    ", videoFrames=" + videoFrames +
                    ", timestamp=" + timestamp +
                    ", frameNumber=" + frameNumber +
                    ", imageWidth=" + imageWidth +
                    ", imageHeight=" + imageHeight +
                    ", numColumns=" + numColumns +
                    ", numRows=" + numRows +
                    '}';
        }

        public VideoFrame encodeVideoFrame() {
            log.info("encodeVideoFrame: BEGIN: frameNumber={}", frameNumber);
            final ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, numColumns, numRows);
            videoFrames.forEach((position, frame) -> builder.addImage(position, frame.data));
            final long t0 = System.currentTimeMillis();
            VideoFrame videoFrame = new VideoFrame();
            videoFrame.camera = monitor;
            videoFrame.ssrc = ssrc;
            videoFrame.timestamp = timestamp;
            videoFrame.frameNumber = frameNumber;
            videoFrame.data = builder.getOutputImageBytes("jpg");
            videoFrame.hash = videoFrame.calculateHash();
            videoFrame.tags = new HashMap<>();
            videoFrame.tags.put("cameras", videoFrames.values().stream().map(f -> f.camera).collect(Collectors.toList()).toString());
            videoFrame.tags.put("numCameras", Integer.toString(videoFrames.size()));
            log.info("encodeVideoFrame: END: frameNumber={}, TIME={}", frameNumber, System.currentTimeMillis() - t0);
            log.trace("encodeVideoFrame: videoFrame={}", videoFrame);
            return videoFrame;
        }
    }
}
