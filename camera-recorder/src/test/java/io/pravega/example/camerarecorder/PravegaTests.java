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

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameGrabber;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_videoio.VideoCapture;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;

public class PravegaTests {
    private static Logger log = LoggerFactory.getLogger(PravegaTests.class);

    @Test
    @Ignore
    public void Test1() throws Exception {
        final URI controllerURI = new URI("tcp://10.246.21.231:9090");
        String scope = "examples";
        String streamName = "video1";
        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<String> pravegaWriter = clientFactory.createEventWriter(
                     streamName,
                     new UTF8StringSerializer(),
                     EventWriterConfig.builder().build())) {
            pravegaWriter.writeEvent("0", "{\"camera\":0}").get();
        }
        log.info("Done.");
    }

    @Test
    @Ignore
    public void Test2() throws Exception {
        Mat image = opencv_imgcodecs.imread("c:\\temp\\capture.png");
        log.info("image={}", image);
        BytePointer png = new BytePointer();
        opencv_imgcodecs.imencode(".png", image,  png);
        log.info("png={}", png);
        byte[] pngByteArray = png.getStringBytes();
        Files.write((new File("c:\\temp\\capture2.png")).toPath(), pngByteArray);
    }

    @Test
    @Ignore
    public void Test3() throws Exception {
        VideoCapture cap = new VideoCapture();
        cap.open(1);
        Mat image = new Mat();
        log.info("Reading image from camera");
        cap.read(image);
        log.info("image={}", image);
        opencv_imgcodecs.imwrite("c:\\temp\\capture3.png", image);
    }

    @Test
    @Ignore
    public void Test4() throws Exception {
        final int WEBCAM_DEVICE_INDEX = 1;
        final int captureWidth = 640;
        final int captureHeight = 480;

        // The available FrameGrabber classes include OpenCVFrameGrabber (opencv_videoio),
        // DC1394FrameGrabber, FlyCapture2FrameGrabber, OpenKinectFrameGrabber,
        // PS3EyeFrameGrabber, VideoInputFrameGrabber, and FFmpegFrameGrabber.
        final OpenCVFrameGrabber grabber = new OpenCVFrameGrabber(WEBCAM_DEVICE_INDEX);
        grabber.setImageWidth(captureWidth);
        grabber.setImageHeight(captureHeight);
        grabber.setFrameRate(15.0);
        log.info("getFrameRate={}", grabber.getFrameRate());
        grabber.start();
        log.info("getFrameRate={}", grabber.getFrameRate());

        final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / grabber.getGamma());
        Frame capturedFrame = null;
        while ((capturedFrame = grabber.grab()) != null)
        {
            long timestamp = System.currentTimeMillis();
            log.info("timestamp={}, capturedFrame={}", timestamp, capturedFrame);

            if (cFrame.isVisible())
            {
                // Show our frame in the preview
                cFrame.showImage(capturedFrame);
            }
            Thread.sleep(500);
        }
    }
}
