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

import io.pravega.example.flinkprocessor.AppConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configuration class used for all video jobs in this project.
 * This class can be extended for job-specific configuration parameters.
 */
public class VideoAppConfiguration extends AppConfiguration {
    private static Logger log = LoggerFactory.getLogger(VideoAppConfiguration.class);

    private final int numCameras;
    private final int imageWidth;
    private final int chunkSizeBytes;
    private final boolean dropChunks;
    private final double framesPerSec;

    public VideoAppConfiguration(String[] args) {
        super(args);
        numCameras = getParams().getInt("numCameras", 4);
        imageWidth = getParams().getInt("imageWidth", 100);
        chunkSizeBytes = getParams().getInt("chunkSizeBytes", 10*1024);
        dropChunks = getParams().getBoolean("dropChunks", false);
        framesPerSec = getParams().getDouble("framesPerSec", 1.0);
    }

    @Override
    public String toString() {
        return "VideoAppConfiguration{" +
                super.toString() +
                ", numCameras=" + numCameras +
                ", imageWidth=" + imageWidth +
                ", chunkSizeBytes=" + chunkSizeBytes +
                ", dropChunks=" + dropChunks +
                ", framesPerSec=" + framesPerSec +
                '}';
    }

    public int getNumCameras() {
        return numCameras;
    }

    public int getImageWidth() {
        return imageWidth;
    }

    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    public boolean isDropChunks() {
        return dropChunks;
    }

    public double getFramesPerSec() {
        return framesPerSec;
    }
}
