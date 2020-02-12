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

import io.pravega.example.common.ChunkedVideoFrame;
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
        if (element.chunkIndex == element.finalChunkIndex) {
            // If we have the last chunk, fire immediately.
            log.trace("onElement: FIRE_AND_PURGE; final chunk; element={}, timestamp={}, window={}, getCurrentWatermark={}",
                    element, timestamp, window, ctx.getCurrentWatermark());
            return TriggerResult.FIRE_AND_PURGE;
        }
        else if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // If the watermark is already past the window, fire immediately.
            log.trace("onElement: FIRE_AND_PURGE; watermark is past window; element={}, timestamp={}, window={}, getCurrentWatermark={}",
                    element, timestamp, window, ctx.getCurrentWatermark());
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            log.trace("onElement: CONTINUE; element={}, timestamp={}, window={}, getCurrentWatermark={}",
                    element, timestamp, window, ctx.getCurrentWatermark());
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, VideoFrameWindow window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            log.trace("onEventTime: FIRE_AND_PURGE; time={}, window={}", time, window);
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            log.trace("onEventTime: CONTINUE; time={}, window={}", time, window);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, VideoFrameWindow window, TriggerContext ctx) {
        log.trace("onProcessingTime: CONTINUE; time={}, window={}", time, window);
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(VideoFrameWindow window, TriggerContext ctx) {
    }
}
