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

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Iterator;

/**
 * An Iterator that returns sequential frame numbers and timestamps.
 * Elements are rate limited at the specified framesPerSec.
 * Used to generate sample video data.
 */
public class FrameNumberIterator implements Iterator<Tuple2<Integer,Long>>, Serializable {
    private final double framesPerSec;
    private final long burstFrames;
    private int frameNumber;
    private transient Bucket tokenBucket;

    public FrameNumberIterator(double framesPerSec, long burstFrames) {
        this.framesPerSec = framesPerSec;
        this.burstFrames = burstFrames;
        initializeTokenBucket();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initializeTokenBucket();
    }

    /**
     * Create token bucket to allow a burst of burstFrames and a normal rate of framesPerSec.
     * 1 token = 1 frame
     */
    private void initializeTokenBucket() {
        final double burstSec = burstFrames / framesPerSec;
        final Bandwidth limit = Bandwidth.simple(burstFrames, Duration.ofMillis((long) (1000*burstSec)));
        tokenBucket = Bucket4j.builder().addLimit(limit).build();
        // Start with an empty bucket to allow a ramp up.
        tokenBucket.tryConsumeAsMuchAsPossible();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Tuple2<Integer,Long> next() {
        // Wait for the token bucket to fill up. This limits the maximum rate.
        tokenBucket.asScheduler().consumeUninterruptibly(1);
        long timeMs = System.currentTimeMillis();
        Tuple2<Integer,Long> result = new Tuple2<Integer,Long>(frameNumber, timeMs);
        frameNumber++;
        return result;
    }
}
