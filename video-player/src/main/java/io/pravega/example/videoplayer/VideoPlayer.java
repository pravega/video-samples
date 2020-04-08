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
package io.pravega.example.videoplayer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.VideoFrame;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Reads video images a Pravega stream and displays them on the screen.
 */
public class VideoPlayer implements Runnable {
    private static Logger log = LoggerFactory.getLogger(VideoPlayer.class);

    private final AppConfiguration config;

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new VideoPlayer(config);
        app.run();
    }

    public VideoPlayer(AppConfiguration appConfiguration) {
        config = appConfiguration;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() {
        try {
            String scope = getConfig().getInputStreamConfig().getStream().getScope();
            String streamName = getConfig().getInputStreamConfig().getStream().getStreamName();
            ClientConfig clientConfig = getConfig().getClientConfig();

            StreamInfo streamInfo;
            try (StreamManager streamManager = StreamManager.create(clientConfig)) {
                streamInfo = streamManager.getStreamInfo(scope, streamName);
            }

            StreamCut startStreamCut = getConfig().getInputStreamConfig().getStartStreamCut();
            if (startStreamCut == StreamCut.UNBOUNDED && getConfig().isStartAtTail()) {
                startStreamCut = streamInfo.getTailStreamCut();
            }

            final String readerGroup = UUID.randomUUID().toString().replace("-", "");
            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(
                            getConfig().getInputStreamConfig().getStream(),
                            startStreamCut,
                            getConfig().getInputStreamConfig().getEndStreamCut())
                    .build();
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
                readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            }

            final long timeoutMs = 1000;
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            log.info("gamma={}", CanvasFrame.getDefaultGamma());
            final CanvasFrame cFrame = new CanvasFrame("Playback from Pravega", 1.0);
            OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();

            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                 EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                         readerGroup,
                         new ByteBufferSerializer(),
                         ReaderConfig.builder().build())) {
                final StreamCutBuilder streamCutBuilder = new StreamCutBuilder(getConfig().getInputStreamConfig().getStream(), startStreamCut);
                for (;;) {
                    EventRead<ByteBuffer> event = reader.readNextEvent(timeoutMs);
                    if (event.getEvent() != null) {
                        streamCutBuilder.addEvent(event.getPosition());
                        final StreamCut streamCutForNextEvent = streamCutBuilder.getStreamCut();
                        final ChunkedVideoFrame chunkedVideoFrame = mapper.readValue(event.getEvent().array(), ChunkedVideoFrame.class);
                        log.info("chunkedVideoFrame={}", chunkedVideoFrame);
                        log.info("streamCutForNextEvent={}", streamCutForNextEvent);
                        log.info("streamCutForNextEvent={}", streamCutForNextEvent.asText());
                        // TODO: Reassemble multiple chunks - see ChunkedVideoFrameReassembler
                        final VideoFrame videoFrame = new VideoFrame(chunkedVideoFrame);
                        if (videoFrame.camera == getConfig().getCamera()) {
                            videoFrame.validateHash();
                            log.info("data length is " + videoFrame.data.length);
                            final Mat pngMat = new Mat(new BytePointer(videoFrame.data));
                            final Mat mat = opencv_imgcodecs.imdecode(pngMat, opencv_imgcodecs.IMREAD_UNCHANGED);
                            log.info("mat length is " + mat.dims());
                            final Frame frame = converter.convert(mat);
                            cFrame.showImage(frame);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
