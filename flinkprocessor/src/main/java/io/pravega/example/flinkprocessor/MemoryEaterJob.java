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
package io.pravega.example.flinkprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Uses excessive amounts of memory to force an out-of-memory error.
 */
public class MemoryEaterJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MemoryEaterJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        MemoryEaterJob job = new MemoryEaterJob(config);
        job.run();
    }

    public MemoryEaterJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MemoryEaterJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getOutputStreamConfig());
            final StreamCut startStreamCut = resolveStartStreamCut(getConfig().getInputStreamConfig());
            final StreamCut endStreamCut = resolveEndStreamCut(getConfig().getInputStreamConfig());
            FlinkPravegaReader<InputType> flinkPravegaReader = FlinkPravegaReader.<InputType>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new JsonDeserializationSchema<>(InputType.class))
                    .build();
            DataStream<InputType> ds = env.addSource(flinkPravegaReader);
            ds.printToErr();

            DataStream<InputType> ds2 = ds.map((event) -> {
                final long totalBytes = event.size;
                final double ageSec = (System.currentTimeMillis() - event.timestamp) / 1000.0;
                log.info("event={}, ageSec={}", event, ageSec);
                // Only eat memory if event is younger than 5 seconds.
                // This should allow the processing of this event to succeed upon recovery from OOM.
                if (ageSec < 5.0) {
                    final int bytesPerBuffer = 1024 * 1024;
                    final int numBuffers = (int) (totalBytes / bytesPerBuffer);
                    log.info("Allocating {} buffers of {} bytes each.", numBuffers, bytesPerBuffer);
                    final byte[][] buf = new byte[numBuffers][bytesPerBuffer];
                    log.info("Allocated {} buffers of {} bytes each.", numBuffers, bytesPerBuffer);
                }
                return event;
            });
            ds2.printToErr();

            FlinkPravegaWriter<InputType> sink = FlinkPravegaWriter.<InputType>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new JsonSerializationSchema<>())
                    .withEventRouter((s) -> "")
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            ds2
                    .addSink(sink)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class InputType {
        public long size;
        public long timestamp;

        @Override
        public String toString() {
            return "InputType{" +
                    "size=" + size +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
