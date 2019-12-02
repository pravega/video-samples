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
import io.pravega.example.flinkprocessor.JsonSerializationSchema;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.SensorReading;
import io.pravega.example.common.VideoFrame;
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
 * This job simulates writing non-video sensor data to Pravega.
 */
public class SensorDataGeneratorJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(SensorDataGeneratorJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        SensorDataGeneratorJob job = new SensorDataGeneratorJob(config);
        job.run();
    }

    public SensorDataGeneratorJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = SensorDataGeneratorJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            if (getConfig().isWriteToPravega()) {
                createStream(getConfig().getOutputStreamConfig());
                }

            // Generate a stream of sequential timestamps.
            DataStream<Tuple2<Integer,Long>> frameNumbers = env.fromCollection(
                    new FrameNumberIterator(getConfig().getFramesPerSec(), 10),
                    TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>(){}))
                    .uid("frameNumbers")
                    .name("frameNumbers");

            // Generate a stream of SensorReading instances.
            int[] cameras = IntStream.range(0, getConfig().getNumCameras()).toArray();
            DataStream<SensorReading> sensorReadings =
                    frameNumbers.flatMap(new FlatMapFunction<Tuple2<Integer,Long>, SensorReading>() {
                        @Override
                        public void flatMap(Tuple2<Integer,Long> in, Collector<SensorReading> out) {
                            for (int camera: cameras) {
                                SensorReading sensorReading = new SensorReading();
                                sensorReading.sensorId = camera * 1000;
                                sensorReading.camera = camera;
                                sensorReading.timestamp = new Timestamp(in.f1);
                                sensorReading.temperatureCelsius = (double) in.f0;
                                sensorReading.humidity = (double) in.f0;
                                out.collect(sensorReading);
                            }
                        }
                    })
                    .setParallelism(1)
                    .uid("sensorReadings")
                    .name("sensorReadings");
            sensorReadings.printToErr().uid("sensorReadings-print").name("sensorReadings-print");

            // Write sensor readings to Pravega encoded as JSON.
            if (getConfig().isWriteToPravega()) {
                FlinkPravegaWriter<SensorReading> sink = FlinkPravegaWriter.<SensorReading>builder()
                        .withPravegaConfig(getConfig().getPravegaConfig())
                        .forStream(getConfig().getOutputStreamConfig().getStream())
                        .withSerializationSchema(new JsonSerializationSchema<SensorReading>())
                        .withEventRouter(sensorReading -> String.format("%d", sensorReading.camera))
                        .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                        .build();
                sensorReadings
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
