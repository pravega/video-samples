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
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.KittiSensorReading;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.JsonDeserializationSchema;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This job demonstrates how to join different data types from different Pravega streams.
 */
public class KittiSensorFusionJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(KittiSensorFusionJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        KittiSensorFusionJob job = new KittiSensorFusionJob(config);
        job.run();
    }

    public KittiSensorFusionJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final String jobName = KittiSensorFusionJob.class.getName();
            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getSensorStreamConfig());
            createStream(getConfig().getOutputStreamConfig());

            //
            // Create video datastream.
            //

            final StreamCut startStreamCut;
            if (getConfig().isStartAtTail()) {
                startStreamCut = getStreamInfo(getConfig().getInputStreamConfig().getStream()).getTailStreamCut();
            } else {
                startStreamCut = StreamCut.UNBOUNDED;
            }

            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");

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

            // Unchunk (disabled).
            // Operator: ChunkedVideoFrameReassembler
            final DataStream<VideoFrame> videoFrames = inChunkedVideoFramesWithTimestamps
                    .map(VideoFrame::new)
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");

            //
            // Create non-video sensor datastream.
            //

            final StreamCut startStreamCutSensor;
            if (getConfig().isStartAtTail()) {
                startStreamCutSensor = getStreamInfo(getConfig().getSensorStreamConfig().getStream()).getTailStreamCut();
            } else {
                startStreamCutSensor = StreamCut.UNBOUNDED;
            }

            final FlinkPravegaReader<KittiSensorReading> flinkPravegaReaderSensor = FlinkPravegaReader.<KittiSensorReading>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getSensorStreamConfig().getStream(), startStreamCutSensor, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new JsonDeserializationSchema<>(KittiSensorReading.class))
                    .build();
            final DataStream<KittiSensorReading> inSensorReadings = env
                    .addSource(flinkPravegaReaderSensor)
                    .uid("input-sensor")
                    .name("input-sensor");
            inSensorReadings.printToErr().uid("inSensorReadings-print").name("inSensorReadings-print");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            final DataStream<KittiSensorReading> inSensorReadingsWithTimestamps = inSensorReadings
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<KittiSensorReading>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(KittiSensorReading element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarksSensor")
                    .name("assignTimestampsAndWatermarksSensor");
            inSensorReadingsWithTimestamps.printToErr().uid("inSensorReadingsWithTimestamps-print").name("inSensorReadingsWithTimestamps-print");

            // For each camera and window, get the most recent frame.
            final DataStream<VideoFrame> lastVideoFramePerCamera = videoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");

            // For each camera and window, get the most recent sensor reading.
            final DataStream<KittiSensorReading> lastSensorReadingsPerCamera = inSensorReadingsWithTimestamps
                    .keyBy((KeySelector<KittiSensorReading, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastSensorReadingsPerCamera")
                    .name("lastSensorReadingsPerCamera");

            //
            // Join video and non-video sensor data.
            //

            final DataStream<VideoFrame> outVideoFrames = lastVideoFramePerCamera
                    .join(lastSensorReadingsPerCamera)
                    .where((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .equalTo((KeySelector<KittiSensorReading, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .apply((JoinFunction<VideoFrame, KittiSensorReading, VideoFrame>) (first, second) -> {
                        first.kittiSensorReadings = second;
                        return first;
                    });
            outVideoFrames.printToErr();

            // Split output video frames into chunks of 8 MiB or less.
            final DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Write chunks to Pravega encoded as JSON.
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
}
