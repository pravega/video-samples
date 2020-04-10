package io.pravega.example.videoprocessor;

import io.pravega.example.common.VideoFrame;
import org.apache.flink.api.java.functions.KeySelector;

public class OrderedVideoFrame /*implements KeySelector<OrderedVideoFrame, Integer>*/ {
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

//    @Override
//    public Integer getKey(OrderedVideoFrame value) throws Exception {
//        return value.value.camera;
//    }
}
