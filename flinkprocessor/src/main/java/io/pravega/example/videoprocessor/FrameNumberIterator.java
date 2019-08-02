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

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * An Iterator that returns sequential frame numbers and timestamps.
 * Elements are rate limited at the specified framesPerSec.
 * Used to generate sample video data.
 */
public class FrameNumberIterator implements Iterator<Tuple2<Integer,Long>>, Serializable {
    private double framesPerSec;
    private int frameNumber;
    private long t0;

    public FrameNumberIterator(double framesPerSec) {
        this.framesPerSec = framesPerSec;
        this.frameNumber = 0;
        this.t0 = System.currentTimeMillis();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Tuple2<Integer,Long> next() {
        long timeMs = t0 + (long) (frameNumber * framesPerSec * 1000);
        long sleepMs = timeMs - System.currentTimeMillis();
        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple2<Integer,Long> result = new Tuple2<Integer,Long>(frameNumber, timeMs);
        frameNumber++;
        return result;
    }
}
