package io.pravega.example.videoprocessor;

import io.pravega.example.common.VideoFrame;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class BadgesCoProcessor extends CoProcessFunction<VideoFrame, Set, VideoFrame> {
    private static Logger log = LoggerFactory.getLogger(BadgesCoProcessor.class);

    private ValueState<Set> lastBadges;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastBadges = getRuntimeContext().getState(new ValueStateDescriptor<>("lastBadges", Set.class));
    }

    @Override
    public void processElement1(VideoFrame value, Context ctx, Collector<VideoFrame> out) throws Exception {
        log.info("processElement1 badges:{}", lastBadges.value());
        value.lastBadges = lastBadges.value();
        value.hash = value.calculateHash();

        log.info(value.toString());

        out.collect(value);
    }

    @Override
    public void processElement2(Set value, Context ctx, Collector<VideoFrame> out) throws Exception {
        lastBadges.update(value);
//        log.info("processElement2 badges:{}", lastBadges.value());
    }
}
