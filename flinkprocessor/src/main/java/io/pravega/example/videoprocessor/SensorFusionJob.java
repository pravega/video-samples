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
import io.pravega.example.flinkprocessor.JsonDeserializationSchema;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.SensorReading;
import io.pravega.example.common.VideoFrame;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * This job demonstrates how to join different data types from different Pravega streams.
 */
public class SensorFusionJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(SensorFusionJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        SensorFusionJob job = new SensorFusionJob(config);
        job.run();
    }

    public SensorFusionJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = SensorFusionJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());

            //
            // Create video datastream.
            //

            final StreamCut startStreamCut = resolveStartStreamCut(getConfig().getInputStreamConfig());
            final StreamCut endStreamCut = resolveEndStreamCut(getConfig().getInputStreamConfig());

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, endStreamCut)
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

            // Reassemble whole video frames from chunks.
            boolean failOnError = false;
            DataStream<VideoFrame> videoFrames = inChunkedVideoFramesWithTimestamps
                    .keyBy("camera")
                    .window(new ChunkedVideoFrameWindowAssigner())
                    .process(new ChunkedVideoFrameReassembler().withFailOnError(failOnError))
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");
            videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            //
            // Create non-video sensor datastream.
            //

            final StreamCut startStreamCutSensor = resolveStartStreamCut(getConfig().getSensorStreamConfig());
            final StreamCut endStreamCutSensor = resolveEndStreamCut(getConfig().getSensorStreamConfig());

            FlinkPravegaReader<SensorReading> flinkPravegaReaderSensor = FlinkPravegaReader.<SensorReading>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getSensorStreamConfig().getStream(), startStreamCutSensor, endStreamCutSensor)
                    .withDeserializationSchema(new JsonDeserializationSchema<>(SensorReading.class))
                    .build();
            DataStream<SensorReading> inSensorReadings = env
                    .addSource(flinkPravegaReaderSensor)
                    .uid("input-sensor")
                    .name("input-sensor");
            inSensorReadings.printToErr().uid("inSensorReadings-print").name("inSensorReadings-print");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            DataStream<SensorReading> inSensorReadingsWithTimestamps = inSensorReadings
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(SensorReading element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarksSensor")
                    .name("assignTimestampsAndWatermarksSensor");
            inSensorReadingsWithTimestamps.printToErr().uid("inSensorReadingsWithTimestamps-print").name("inSensorReadingsWithTimestamps-print");

            //
            // Join video and non-video sensor data.
            //

            long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            videoFrames
                    .join(inSensorReadingsWithTimestamps)
                    .where((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .equalTo((KeySelector<SensorReading, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .apply(new JoinFunction<VideoFrame, SensorReading, String>() {
                        @Override
                        public String join(VideoFrame first, SensorReading second) throws Exception {
                            return "{" + first.toString() + "," + second.toString() + "}";
                        }
                    }).printToErr();

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
