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
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.VideoFrame;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * This job reads a video stream from Pravega and writes frame metadata to the console.
 */
public class VideoReaderJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(VideoReaderJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        VideoReaderJob job = new VideoReaderJob(config);
        job.run();
    }

    public VideoReaderJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = VideoReaderJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());
            final StreamCut startStreamCut = resolveStartStreamCut(getConfig().getInputStreamConfig());
            final StreamCut endStreamCut = resolveEndStreamCut(getConfig().getInputStreamConfig());
            log.info("startStreamCut={}", startStreamCut);
            log.info("endStreamCut={}", endStreamCut);

            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");
            inChunkedVideoFrames.printToErr().uid("inChunkedVideoFrames-print").name("inChunkedVideoFrames-print");

            // Reassemble whole video frames from chunks.
            final DataStream<VideoFrame> videoFrames;
            if (getConfig().isReassembleChunks()) {
                // Assign timestamps and watermarks based on timestamp in each chunk.
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

                final boolean failOnError = false;
                videoFrames = inChunkedVideoFramesWithTimestamps
                        .keyBy("camera")
                        .window(new ChunkedVideoFrameWindowAssigner())
                        .process(new ChunkedVideoFrameReassembler().withFailOnError(failOnError))
                        .uid("ChunkedVideoFrameReassembler")
                        .name("ChunkedVideoFrameReassembler");
            } else {
                // Assume that input is not chuncked.
                videoFrames = inChunkedVideoFrames
                        .map(VideoFrame::new)
                        .uid("ChunkedVideoFrameReassembler")
                        .name("ChunkedVideoFrameReassembler");
            }
            videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            // Write some frames to files for viewing.
            if (false) {
                videoFrames
                        .filter(frame -> frame.frameNumber < 20)
                        .uid("write-file-filter")
                        .map(frame -> {
                            String fileName = String.format("/tmp/camera%d-frame%05d.png", frame.camera, frame.frameNumber);
                            log.info("Writing frame to {}", fileName);
                            try (FileOutputStream fos = new FileOutputStream(fileName)) {
                                fos.write(frame.data);
                            }
                            return 0;
                        })
                        .uid("write-file-map")
                        .name("write-file-map");
            }

            // Parse image file and obtain metadata.
            final DataStream<String> frameInfo = videoFrames
                    .map(frame -> {
                        InputStream inStream = new ByteArrayInputStream(frame.data);
                        BufferedImage inImage = ImageIO.read(inStream);
                        return String.format("camera %d, frame %d, %dx%dx%d, %d bytes, %s",
                                frame.camera,
                                frame.frameNumber,
                                inImage.getWidth(),
                                inImage.getHeight(),
                                inImage.getColorModel().getNumColorComponents(),
                                frame.data.length,
                                inImage.toString());
                    })
                    .uid("frameInfo")
                    .name("frameInfo");
            frameInfo.printToErr().uid("frameInfo-print").name("frameInfo-print");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
