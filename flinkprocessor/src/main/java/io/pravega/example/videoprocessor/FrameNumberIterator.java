package io.pravega.example.videoprocessor;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * An Iterator that returns sequential frame numbers and timestamps.
 * Elements are rate limited at the specified framesPerSec.
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
