package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public class ChunkedVideoFrameWindowAssigner extends WindowAssigner<ChunkedVideoFrame, VideoFrameWindow> {
    private static Logger log = LoggerFactory.getLogger(ChunkedVideoFrameWindowAssigner.class);
    private static final long serialVersionUID = 1L;

    @Override
    public Collection<VideoFrameWindow> assignWindows(ChunkedVideoFrame frame, long timestamp, WindowAssignerContext context) {
        VideoFrameWindow window = new VideoFrameWindow(frame);
        log.trace("assignWindows: window={}, frame={}", window, frame);
        return Collections.singletonList(window);
    }

    @Override
    public Trigger<ChunkedVideoFrame, VideoFrameWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new ChunkedVideoFrameTrigger();
    }

    @Override
    public String toString() {
        return "ChunkedVideoFrameWindowAssigner()";
    }

    @Override
    public TypeSerializer<VideoFrameWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new VideoFrameWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
