package io.pravega.example.videoprocessor;

import io.pravega.example.common.VideoFrame;

/**
 * This is currently unused as time windows appear to work just as well.
 * See MultiVideoGridExperimentalJob.java.
 */
public class OrderedVideoFrame {
    public long index;
    public VideoFrame value;

    public OrderedVideoFrame(long index, VideoFrame value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public String toString() {
        return "OrderedVideoFrame{" +
                "index=" + index +
                ", value=" + value +
                '}';
    }
}