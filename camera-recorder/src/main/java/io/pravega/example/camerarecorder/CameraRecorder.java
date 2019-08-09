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
package io.pravega.example.camerarecorder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.example.video.ChunkedVideoFrame;
import io.pravega.example.video.PravegaUtil;
import io.pravega.example.video.VideoFrame;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Reads video images from a web cam and writes them to a Pravega stream.
 */
public class CameraRecorder implements Runnable {
    private static Logger log = LoggerFactory.getLogger(CameraRecorder.class);

    private final AppConfiguration config;

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new CameraRecorder(config);
        app.run();
    }

    public CameraRecorder(AppConfiguration appConfiguration) {
        config = appConfiguration;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() {
        try {
            // Initialize camera.
            final int captureWidth = getConfig().getImageWidth();
            final int captureHeight = getConfig().getImageHeight();
            log.info("creating grabber");
//        final FrameGrabber grabber = new VideoInputFrameGrabber(WEBCAM_DEVICE_INDEX);
            final FrameGrabber grabber = new OpenCVFrameGrabber(getConfig().getCameraDeviceNumber());
            grabber.setImageWidth(captureWidth);
            grabber.setImageHeight(captureHeight);
            grabber.setFrameRate(getConfig().getFramesPerSec());
            log.info("starting grabber");
            grabber.start();
            log.info("actual frame rate={}", grabber.getFrameRate());

            // Initialize capture preview window.
            final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / grabber.getGamma());

            // Create Pravega stream.
            PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getOutputStreamConfig());

            try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(
                        getConfig().getOutputStreamConfig().getStream().getScope(),
                        getConfig().getClientConfig());
                 EventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createEventWriter(
                         getConfig().getOutputStreamConfig().getStream().getStreamName(),
                         new ByteBufferSerializer(),
                         EventWriterConfig.builder().build())) {

                ObjectMapper mapper = new ObjectMapper();
                OpenCVFrameConverter.ToMat converterToMat = new OpenCVFrameConverter.ToMat();

                int frameNumber = 0;
                int ssrc = new Random().nextInt();
                Frame capturedFrame;

                while ((capturedFrame = grabber.grab()) != null) {
                    long timestamp = System.currentTimeMillis();
                    log.info("frameNumber={}, timestamp={}, capturedFrame={}", frameNumber, timestamp, capturedFrame);

                    // Convert captured frame to PNG.
                    Mat mat = converterToMat.convert(capturedFrame);
                    BytePointer pngBytePointer = new BytePointer();
                    opencv_imgcodecs.imencode(".png", mat,  pngBytePointer);
                    log.info("pngBytePointer={}", pngBytePointer);
                    byte[] pngByteArray = pngBytePointer.getStringBytes();
                    if (false) {
                        Files.write((new File(String.format("capture-%05d.png", frameNumber))).toPath(), pngByteArray);
                    }

                    // Create VideoFrame. We assume that it fits in a single chunk (< 1 MB).
                    VideoFrame videoFrame = new VideoFrame();
                    videoFrame.camera = getConfig().getCamera();
                    videoFrame.ssrc = ssrc;
                    videoFrame.timestamp = new Timestamp(timestamp);
                    videoFrame.frameNumber = frameNumber;
                    videoFrame.data = pngByteArray;
                    videoFrame.hash = videoFrame.calculateHash();
                    ChunkedVideoFrame chunkedVideoFrame = new ChunkedVideoFrame(videoFrame);
                    ByteBuffer jsonBytes = ByteBuffer.wrap(mapper.writeValueAsBytes(chunkedVideoFrame));

                    // Write to Pravega.
                    CompletableFuture<Void> future = pravegaWriter.writeEvent(Integer.toString(videoFrame.camera), jsonBytes);

                    // Show our frame in the preview window..
                    if (cFrame.isVisible()) {
                        cFrame.showImage(capturedFrame);
                    }

                    // Sleep to limit frame rate.
                    Thread.sleep((long) (1000.0 / getConfig().getFramesPerSec()));

                    // Make sure frame has been durably persisted to Pravega.
                    future.get();

                    frameNumber++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
