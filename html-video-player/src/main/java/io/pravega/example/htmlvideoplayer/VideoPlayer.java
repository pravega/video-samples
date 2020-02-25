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
package io.pravega.example.htmlvideoplayer;

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
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Reads video images from a Pravega stream and displays them on a simple HTML page.
 */
public class VideoPlayer implements Runnable {
    private static Logger log = LoggerFactory.getLogger(VideoPlayer.class);

    private final AppConfiguration config;

    /**
     * Map from camera to the latest VideoFrame from camera.
     * Caller must synchronize on this variable.
     */
    private final static Map<Integer, VideoFrame> latestFrames = new HashMap<>();

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

    public static VideoFrame getLatestVideoFrame(int camera) {
        synchronized (latestFrames) {
            return latestFrames.get(camera);
        }
    }

    public HttpServer startHttpServer() {
        // Create a resource config that scans for JAX-RS resources and providers.
        final ResourceConfig rc = new ResourceConfig().packages("io.pravega.example.htmlvideoplayer");

        // Create and start a new instance of grizzly http server exposing the Jersey application.
        final HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(config.getServerURI(), rc);
        HttpHandler staticHttpHandler = new CLStaticHttpHandler(VideoPlayer.class.getClassLoader());
        httpServer.getServerConfiguration().addHttpHandler(staticHttpHandler, "/static");
        return httpServer;
    }

    /**
     * Pump video frames from Pravega to memory.
     */
    public void pravegaReader() {
        String scope = getConfig().getInputStreamConfig().getStream().getScope();
        String streamName = getConfig().getInputStreamConfig().getStream().getStreamName();
        ClientConfig clientConfig = getConfig().getClientConfig();
        StreamInfo streamInfo;
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            streamInfo = streamManager.getStreamInfo(scope, streamName);
        }
        final long timeoutMs = 1000;
        ObjectMapper mapper = new ObjectMapper();
        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(
                        getConfig().getInputStreamConfig().getStream(),
                        streamInfo.getTailStreamCut(),
                        StreamCut.UNBOUNDED)
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
            try {
                try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
                     EventStreamReader<ByteBuffer> reader = clientFactory.createReader("reader",
                             readerGroup,
                             new ByteBufferSerializer(),
                             ReaderConfig.builder().build())) {
                    for (; ; ) {
                        EventRead<ByteBuffer> event = reader.readNextEvent(timeoutMs);
                        if (event.getEvent() != null) {
                            ChunkedVideoFrame chunkedVideoFrame = null;
                            try {
                                chunkedVideoFrame = mapper.readValue(event.getEvent().array(), ChunkedVideoFrame.class);
                            } catch (IOException e) {
                                log.warn("Error parsing event", e);
                                continue;
                            }
                            log.info("chunkedVideoFrame={}", chunkedVideoFrame);
                            VideoFrame videoFrame = new VideoFrame(chunkedVideoFrame);
                            synchronized (latestFrames) {
                                latestFrames.put(videoFrame.camera, videoFrame);
                            }
                        }
                    }
                }
            } finally {
                readerGroupManager.deleteReaderGroup(readerGroup);
            }
        }
    }

    public void run() {
        try {
            final HttpServer httpServer = startHttpServer();
            log.info("Gateway running at {}", config.getServerURI());
            pravegaReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
