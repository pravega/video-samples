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
package io.pravega.example.common;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * A generic configuration class for non-Flink Pravega applications.
 * This class can be extended for application-specific configuration parameters.
 */
public class PravegaAppConfiguration {
    private static Logger log = LoggerFactory.getLogger(PravegaAppConfiguration.class);

    private final ClientConfig clientConfig;
    private final String defaultScope;
    private final StreamConfig inputStreamConfig;
    private final StreamConfig outputStreamConfig;
    private final boolean startAtTail;
    private final boolean pravega_standalone;



    public PravegaAppConfiguration(String[] args) {

//        URI controllerURI = URI.create(getEnvVar("PRAVEGA_CONTROLLER_URI", "tcp://10.247.115.55:9090"));
        URI controllerURI = URI.create(getEnvVar("PRAVEGA_CONTROLLER_URI", "tcp://localhost:9090"));
        System.out.println("fkin controlleruri " + controllerURI);
        clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        defaultScope = getEnvVar("PRAVEGA_SCOPE", "video-demo");
        inputStreamConfig = new StreamConfig(defaultScope,"INPUT_");
        outputStreamConfig = new StreamConfig(defaultScope,"OUTPUT_");
        startAtTail = Boolean.parseBoolean(getEnvVar("START_AT_TAIL", "true"));
        pravega_standalone = Boolean.parseBoolean(getEnvVar("PRAVEGA_STANDALONE", "true"));   //changed

    }

    @Override
    public String toString() {
        return "PravegaAppConfiguration{" +
                "clientConfig=" + clientConfig +
                ", defaultScope='" + defaultScope + '\'' +
                ", inputStreamConfig=" + inputStreamConfig +
                ", outputStreamConfig=" + outputStreamConfig +
                ", startAtTail=" + startAtTail +
                '}';
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public String getDefaultScope() {
        return defaultScope;
    }


    public StreamConfig getInputStreamConfig() {
        return inputStreamConfig;
    }

    public StreamConfig getOutputStreamConfig() {
        return outputStreamConfig;
    }

    public boolean isStartAtTail() {
        return startAtTail;
    }

    public boolean isPravegaStandalonel() {
        return pravega_standalone;
    }

    public static class StreamConfig {
        private final Stream stream;
        private final int targetRate;
        private final int scaleFactor;
        private final int minNumSegments;

        public StreamConfig(String defaultScope, String prefix) {
            String streamName = getEnvVar(prefix + "STREAM_NAME", "video-demo-stream");
            stream = Stream.of(defaultScope, streamName);
            targetRate = Integer.parseInt(getEnvVar(prefix + "TARGET_RATE_KB_PER_SEC", "100000"));
            scaleFactor = Integer.parseInt(getEnvVar(prefix + "SCALE_FACTOR", "2"));
            minNumSegments = Integer.parseInt(getEnvVar(prefix + "MIN_NUM_SEGMENTS", "1"));
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

    protected static String getEnvVar(String name, String defaultValue) {
//        System.out.println(System.getenv("controllerURI"));
        String value = System.getProperty(name);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}