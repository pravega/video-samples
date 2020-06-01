package io.pravega.example.videoprocessor;

import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class BadgesProcessor extends KeyedBroadcastProcessFunction<String, VideoFrame, Set, VideoFrame> {
    private final Logger log = LoggerFactory.getLogger(BadgesProcessor.class);

    MapStateDescriptor<Void, Set> badgeDesc;

    @Override
    public void open(Configuration conf) throws Exception {
        // initialize keyed state
        badgeDesc =
                new MapStateDescriptor<Void, Set>("badgesBroadcastState", Void.class, Set.class);
    }
    @Override
    public void processElement(VideoFrame value, ReadOnlyContext ctx, Collector<VideoFrame> out) throws Exception {
        Set lastBadges = ctx
                .getBroadcastState(this.badgeDesc)
                .get(null);

        log.info("processing badges:{}", lastBadges);

        value.lastBadges = lastBadges;
        value.hash = value.calculateHash();

        out.collect(value);
    }

    @Override
    public void processBroadcastElement(Set value, Context ctx, Collector<VideoFrame> out) throws Exception {
        BroadcastState<Void, Set> bcState = ctx.getBroadcastState(badgeDesc);
        bcState.clear();

        if(value.size() > 0) {
            bcState.put(null, value);
            log.info("trying to add element:{}", value);
        }
    }
}
