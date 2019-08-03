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

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract job class for Flink Pravega applications.
 */
public abstract class AbstractJob implements Runnable {
    private static Logger log = LoggerFactory.getLogger(AbstractJob.class);

    private final AppConfiguration config;

    public AbstractJob(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    /**
     * If the Pravega stream does not exist, creates a new stream with the specified stream configuration.
     * If the stream exists, it is unchanged.
     */
    public void createStream(AppConfiguration.StreamConfig streamConfig) {
        try (StreamManager streamManager = StreamManager.create(getConfig().getPravegaConfig().getClientConfig())) {
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(streamConfig.getScalingPolicy())
                    .build();
            streamManager.createStream(
                    streamConfig.getStream().getScope(),
                    streamConfig.getStream().getStreamName(),
                    streamConfiguration);
        }
    }

    /**
     * Get head and tail stream cuts for a Pravega stream.
     */
    public StreamInfo getStreamInfo(Stream stream) {
        try (StreamManager streamManager = StreamManager.create(getConfig().getPravegaConfig().getClientConfig())) {
            return streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());
        }
    }

    public StreamExecutionEnvironment initializeFlinkStreaming() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = getConfig().getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        if (!getConfig().isEnableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        if(getConfig().isEnableCheckpoint()) {
            long checkpointInterval = getConfig().getCheckpointInterval();
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        }
        log.info("Parallelism={}, MaxParallelism={}", env.getParallelism(), env.getMaxParallelism());

        // We can't use MemoryStateBackend because it can't store large state.
        if (env instanceof LocalStreamEnvironment && (env.getStateBackend() == null || env.getStateBackend() instanceof MemoryStateBackend)) {
            log.warn("Using FsStateBackend instead of MemoryStateBackend");
            env.setStateBackend(new FsStateBackend("file:///tmp/flink-state", true));
        }

        // Stop immediately on any errors.
        if (env instanceof LocalStreamEnvironment) {
            log.warn("Using noRestart restart strategy");
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    public ExecutionEnvironment initializeFlinkBatch() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = getConfig().getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        log.info("Parallelism={}", env.getParallelism());
        return env;
    }
}
