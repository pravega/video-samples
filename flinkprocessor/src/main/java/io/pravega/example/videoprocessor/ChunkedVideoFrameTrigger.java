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

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Trigger that immediately fires when the last chunk for a frame is received.
 */
public class ChunkedVideoFrameTrigger extends Trigger<ChunkedVideoFrame, VideoFrameWindow> {
    private static Logger log = LoggerFactory.getLogger(ChunkedVideoFrameTrigger.class);

    @Override
    public TriggerResult onElement(ChunkedVideoFrame element, long timestamp, VideoFrameWindow window, TriggerContext ctx) {
        log.info("onElement: element={}, timestamp={}, window={}, getCurrentWatermark={}", element, timestamp, window, ctx.getCurrentWatermark());
        if (element.chunkIndex == element.finalChunkIndex)
            return TriggerResult.FIRE_AND_PURGE;
        else if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // If the watermark is already past the window, fire immediately.
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, VideoFrameWindow window, TriggerContext ctx) {
        log.info("onEventTime: time={}, window={}", time, window);
//        return TriggerResult.CONTINUE;
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE_AND_PURGE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, VideoFrameWindow window, TriggerContext ctx) {
        log.info("onProcessingTime: time={}, window={}", time, window);
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(VideoFrameWindow window, TriggerContext ctx) {
    }

//    @Override
//    public boolean canMerge() {
//        return true;
//    }
//
//    @Override
//    public void onMerge(VideoFrameWindow window, OnMergeContext ctx) {
//    }
}
