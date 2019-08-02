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
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.AppConfiguration;
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

    public VideoReaderJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = VideoReaderJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getInputStreamConfig());

            // Start at the current tail.
            StreamCut startStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
//            StreamCut startStreamCut = StreamCut.UNBOUNDED;

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream, startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");
//            inChunkedVideoFrames.printToErr().uid("inChunkedVideoFrames-print").name("inChunkedVideoFrames-print");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFrames
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<ChunkedVideoFrame>(Time.milliseconds(1000)) {
                                @Override
                                public long extractTimestamp(ChunkedVideoFrame element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarks")
                    .name("assignTimestampsAndWatermarks");
//            inChunkedVideoFramesWithTimestamps.printToErr().uid("inChunkedVideoFramesWithTimestamps-print").name("inChunkedVideoFramesWithTimestamps-print");

            // Reassemble whole video frames from chunks.
            boolean failOnError = true;
            DataStream<VideoFrame> videoFrames = inChunkedVideoFramesWithTimestamps
                    .keyBy("camera")
                    .window(new ChunkedVideoFrameWindowAssigner())
                    .process(new ChunkedVideoFrameReassembler().withFailOnError(failOnError))
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");
            videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            // Write some frames to files for viewing.
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

            // Parse image file and obtain metadata.
            DataStream<String> frameInfo = videoFrames
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
