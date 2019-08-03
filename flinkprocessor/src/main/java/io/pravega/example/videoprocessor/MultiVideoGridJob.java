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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getOutputStreamConfig());

            StreamCut startStreamCut = StreamCut.UNBOUNDED;
            if (getConfig().isStartAtTail()) {
                startStreamCut = getStreamInfo(getConfig().getInputStreamConfig().getStream()).getTailStreamCut();
            }

            // Read chunked video frames from Pravega.
            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFrames
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

            // Reassemble whole video frames from chunks.
            boolean failOnError = false;
            DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramesWithTimestamps
                    .keyBy("camera")
                    .window(new ChunkedVideoFrameWindowAssigner())
                    .process(new ChunkedVideoFrameReassembler().withFailOnError(failOnError))
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");
            inVideoFrames.printToErr().uid("inVideoFrames-print").name("inVideoFrames-print");

            // Resize all input images. This will be performed in parallel.
            int imageWidth = getConfig().getImageWidth();
            int imageHeight = imageWidth;
            DataStream<VideoFrame> resizedVideoFrames = inVideoFrames
                    .map(frame -> {
                        ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
                        frame.data = resizer.resize(frame.data);
                        frame.hash = null;
                        return frame;
                    })
                    .uid("ImageResizer")
                    .name("ImageResizer");
//            resizedVideoFrames.printToErr().uid("resizedVideoFrames-print").name("resizedVideoFrames-print");;

            // Aggregate resized images.
            // For each time window, we take the last image from each camera.
            // Then these images are combined in a square grid.
            // To maintain ordering in the output images, we use parallelism of 1 for all subsequent operations.
            long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            int camera = 1000;
            int ssrc = new Random().nextInt();
            DataStream<VideoFrame> outVideoFrames = resizedVideoFrames
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .aggregate(new ImageAggregator(imageWidth, imageHeight, camera, ssrc))
                    .setParallelism(1)
                    .uid("ImageAggregator")
                    .name("ImageAggregator");
            outVideoFrames.printToErr().setParallelism(1).uid("outVideoFrames-print").name("outVideoFrames-print");

            // Split output video frames into chunks of 1 MB or less.
            DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker())
                    .setParallelism(1)
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");
//            outChunkedVideoFrames.printToErr().setParallelism(1).uid("outChunkedVideoFrames-print").name("outChunkedVideoFrames-print");

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            outChunkedVideoFrames
                    .addSink(flinkPravegaWriter)
                    .setParallelism(1)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
        // Maximum timestamp from cameras.
        public Timestamp timestamp = new Timestamp(0);
    }

    public static class ImageAggregator implements AggregateFunction<VideoFrame, ImageAggregatorAccum, VideoFrame> {
        private static Logger log = LoggerFactory.getLogger(ImageAggregator.class);

        private final int imageWidth;
        private final int imageHeight;
        private final int camera;
        private final int ssrc;
        // frameNumber is part of the state. There is only a single partition so this can be an ordinary instance variable.
        // TODO: Store frameNumber in Flink state to maintain value across restarts.
        private int frameNumber;

        public ImageAggregator(int imageWidth, int imageHeight, int camera, int ssrc) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
            this.camera = camera;
            this.ssrc = ssrc;
        }

        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public VideoFrame getResult(ImageAggregatorAccum accum) {
            VideoFrame videoFrame = new VideoFrame();
            videoFrame.camera = camera;
            videoFrame.ssrc = ssrc;
            videoFrame.timestamp = accum.timestamp;
            videoFrame.frameNumber = frameNumber;
            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, accum.images.size());
            builder.addImages(accum.images);
            videoFrame.data = builder.getOutputImageBytes("png");
            videoFrame.hash = videoFrame.calculateHash();
            videoFrame.tags = new HashMap<String,String>();
            videoFrame.tags.put("numCameras", Integer.toString(accum.images.size()));
            frameNumber++;
            log.trace("getResult: videoFrame={}", videoFrame);
            return videoFrame;
        }

        @Override
        public ImageAggregatorAccum add(VideoFrame value, ImageAggregatorAccum accum) {
            log.trace("add: value={}", value);
            accum.images.put(value.camera, value.data);
            accum.timestamp = new Timestamp(max(accum.timestamp.getTime(), value.timestamp.getTime()));
            return accum;
        }

        @Override
        public ImageAggregatorAccum merge(ImageAggregatorAccum a, ImageAggregatorAccum b) {
            // TODO: Accumulator can be made more efficient when multiple frames from the same camera can be merged. For now, don't merge.
            return null;
        }

    }
}
