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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic configuration class for Flink Pravega applications.
 * This class can be extended for job-specific configuration parameters.
 */
public class AppConfiguration {
    private static Logger log = LoggerFactory.getLogger(AppConfiguration.class);

    private final ParameterTool params;
    private final PravegaConfig pravegaConfig;
    private final StreamConfig inputStreamConfig;
    private final StreamConfig outputStreamConfig;
    private final int parallelism;
    private final long checkpointInterval;
    private final boolean enableCheckpoint;
    private final boolean enableOperatorChaining;
    private final boolean enableRebalance;
    private final boolean startAtTail;
    private final long maxOutOfOrdernessMs;

    public AppConfiguration(String[] args) {
        params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", getParams().toMap());
        String defaultScope = getParams().get("scope", "examples");
        pravegaConfig = PravegaConfig.fromParams(getParams()).withDefaultScope(defaultScope);
        inputStreamConfig = new StreamConfig(getPravegaConfig(),"input-",  getParams());
        outputStreamConfig = new StreamConfig(getPravegaConfig(),"output-",  getParams());
        parallelism = getParams().getInt("parallelism", 0);
        checkpointInterval = getParams().getLong("checkpointInterval", 10000);     // milliseconds
        enableCheckpoint = getParams().getBoolean("enableCheckpoint", true);
        enableOperatorChaining = getParams().getBoolean("enableOperatorChaining", true);
        enableRebalance = getParams().getBoolean("rebalance", false);
        startAtTail = getParams().getBoolean("startAtTail", true);
        maxOutOfOrdernessMs = getParams().getLong("maxOutOfOrdernessMs", 1000);
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                "pravegaConfig=" + pravegaConfig +
                ", inputStreamConfig=" + inputStreamConfig +
                ", outputStreamConfig=" + outputStreamConfig +
                ", parallelism=" + parallelism +
                ", checkpointInterval=" + checkpointInterval +
                ", enableCheckpoint=" + enableCheckpoint +
                ", enableOperatorChaining=" + enableOperatorChaining +
                ", enableRebalance=" + enableRebalance +
                ", startAtTail=" + startAtTail +
                ", maxOutOfOrdernessMs=" + maxOutOfOrdernessMs +
                '}';
    }

    public ParameterTool getParams() {
        return params;
    }

    public PravegaConfig getPravegaConfig() {
        return pravegaConfig;
    }

    public StreamConfig getInputStreamConfig() {
        return inputStreamConfig;
    }

    public StreamConfig getOutputStreamConfig() {
        return outputStreamConfig;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    public boolean isEnableOperatorChaining() {
        return enableOperatorChaining;
    }

    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    public boolean isStartAtTail() {
        return startAtTail;
    }

    public long getMaxOutOfOrdernessMs() {
        return maxOutOfOrdernessMs;
    }

    public static class StreamConfig {
        private final Stream stream;
        private final int targetRate;
        private final int scaleFactor;
        private final int minNumSegments;

        public StreamConfig(PravegaConfig pravegaConfig, String argPrefix, ParameterTool params) {
            stream = pravegaConfig.resolve(params.get(argPrefix + "stream", "default"));
            targetRate = params.getInt(argPrefix + "targetRate", 100000);  // data rate in KB/sec
            scaleFactor = params.getInt(argPrefix + "scaleFactor", 2);
            minNumSegments = params.getInt(argPrefix + "minNumSegments", 2);
        }

        @Override
        public String toString() {
            return "StreamConfig{" +
                    "stream=" + stream +
                    ", targetRate=" + targetRate +
                    ", scaleFactor=" + scaleFactor +
                    ", minNumSegments=" + minNumSegments +
                    '}';
        }

        public Stream getStream() {
            return stream;
        }

        public ScalingPolicy getScalingPolicy() {
            return ScalingPolicy.byDataRate(targetRate, scaleFactor, minNumSegments);
        }
    }
}
