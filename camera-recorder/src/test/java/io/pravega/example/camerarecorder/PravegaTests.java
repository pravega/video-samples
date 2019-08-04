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
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_videoio.VideoCapture;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.concurrent.CompletableFuture;

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
//        final FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(WEBCAM_DEVICE_INDEX);
        grabber.setImageWidth(captureWidth);
        grabber.setImageHeight(captureHeight);
        grabber.setFrameRate(15.0);
//        log.info("getFrameRate={}", grabber.getFrameRate());

        grabber.start();

        log.info("actual frame rate={}", grabber.getFrameRate());

        OpenCVFrameConverter.ToMat converterToMat = new OpenCVFrameConverter.ToMat();

        final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / grabber.getGamma());
        Frame capturedFrame;
        int frameNumber = 0;
        while ((capturedFrame = grabber.grab()) != null)
        {
            long timestamp = System.currentTimeMillis();
            log.info("frameNumber={}, timestamp={}, capturedFrame={}", frameNumber, timestamp, capturedFrame);

            Mat mat = converterToMat.convert(capturedFrame);
            BytePointer pngBytePointer = new BytePointer();
            opencv_imgcodecs.imencode(".png", mat,  pngBytePointer);
            log.info("pngBytePointer={}", pngBytePointer);
            byte[] pngByteArray = pngBytePointer.getStringBytes();
            Files.write((new File(String.format("c:\\temp\\capture4-%05d.png", frameNumber))).toPath(), pngByteArray);

            if (cFrame.isVisible())
            {
                // Show our frame in the preview
                cFrame.showImage(capturedFrame);
            }

            Thread.sleep(500);
            frameNumber++;
        }
    }

    @Test
    @Ignore
    public void Test5() throws Exception {
        final int WEBCAM_DEVICE_INDEX = 1;
        final int captureWidth = 320;
        final int captureHeight = 240;

        final OpenCVFrameGrabber grabber = new OpenCVFrameGrabber(WEBCAM_DEVICE_INDEX);
        grabber.setImageWidth(captureWidth);
        grabber.setImageHeight(captureHeight);
        grabber.setFrameRate(15.0);

        grabber.start();

        log.info("actual frame rate={}", grabber.getFrameRate());

        OpenCVFrameConverter.ToMat converterToMat = new OpenCVFrameConverter.ToMat();

        final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / grabber.getGamma());
        Frame capturedFrame;
        int frameNumber = 0;

        ObjectMapper mapper = new ObjectMapper();

        final URI controllerURI = new URI("tcp://10.246.21.231:9090");
        String scope = "examples";
        String streamName = "video1";
        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
             EventStreamWriter<ByteBuffer> pravegaWriter = clientFactory.createEventWriter(
                     streamName,
                     new ByteBufferSerializer(),
                     EventWriterConfig.builder().build())) {

            while ((capturedFrame = grabber.grab()) != null)
            {
                long timestamp = System.currentTimeMillis();
                log.info("frameNumber={}, timestamp={}, capturedFrame={}", frameNumber, timestamp, capturedFrame);

                Mat mat = converterToMat.convert(capturedFrame);
                BytePointer pngBytePointer = new BytePointer();
                opencv_imgcodecs.imencode(".png", mat,  pngBytePointer);
                log.info("pngBytePointer={}", pngBytePointer);
                byte[] pngByteArray = pngBytePointer.getStringBytes();
                Files.write((new File(String.format("c:\\temp\\capture4-%05d.png", frameNumber))).toPath(), pngByteArray);

                VideoFrame videoFrame = new VideoFrame();
                videoFrame.camera = 0;
                videoFrame.ssrc = 0;
                videoFrame.timestamp = new Timestamp(timestamp);
                videoFrame.frameNumber = frameNumber;
                videoFrame.data = pngByteArray;
                videoFrame.hash = videoFrame.calculateHash();
                ChunkedVideoFrame chunkedVideoFrame = new ChunkedVideoFrame(videoFrame);

                ByteBuffer jsonBytes = ByteBuffer.wrap(mapper.writeValueAsBytes(chunkedVideoFrame));

                CompletableFuture<Void> future = pravegaWriter.writeEvent(Integer.toString(videoFrame.camera), jsonBytes);

                if (cFrame.isVisible())
                {
                    // Show our frame in the preview
                    cFrame.showImage(capturedFrame);
                }

                Thread.sleep(3000);

                future.get();

                frameNumber++;
            }

        }

    }
}
