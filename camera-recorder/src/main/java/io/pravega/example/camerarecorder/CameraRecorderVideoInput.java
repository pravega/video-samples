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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.PravegaUtil;
import io.pravega.example.common.VideoFrame;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.global.opencv_imgcodecs;
import org.bytedeco.opencv.global.opencv_videoio;
import org.bytedeco.opencv.opencv_core.CvRect;
import org.bytedeco.opencv.opencv_core.IplImage;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.helper.opencv_imgcodecs.cvSaveImage;

/**
 * Reads video images from a web cam and writes them to a Pravega stream.
 */
public class CameraRecorderVideoInput implements Runnable {
    private static Logger log = LoggerFactory.getLogger(CameraRecorderVideoInput.class);

    private final AppConfiguration config;

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new CameraRecorderVideoInput(config);
        app.run();
    }

    public CameraRecorderVideoInput(AppConfiguration appConfiguration) {
        config = appConfiguration;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() {
        try {
            if (getConfig().isCreateScope()) {
                try (StreamManager streamManager = StreamManager.create(getConfig().getClientConfig())) {
                    streamManager.createScope(getConfig().getDefaultScope());
                }
            }


            // Initialize camera.
            final int captureWidth = getConfig().getImageWidth();
            final int captureHeight = getConfig().getImageHeight();
            log.info("creating grabber");
//            final VideoCapture cap = new VideoCapture("/home/vidyat/Desktop/video-samples/data/sampleVideo.mp4");
            final FFmpegFrameGrabber grabber = new FFmpegFrameGrabber("/home/vidyat/Desktop/video-samples/data/DJI_0023.MP4");
//            final FrameGrabber grabber = new OpenCVFrameGrabber(getConfig().getCameraDeviceNumber());


            log.info("starting grabber");
//            log.info("frame rate wanted: {}", getConfig().getFramesPerSec());
//            grabber.setFrameRate(getConfig().getFramesPerSec());

            grabber.start();
            log.info("grabber frame rate={}", grabber.getFrameRate());

//            if(!cap.open(getConfig().getCameraDeviceNumber())) {
//                throw new ConnectIOException("Cannot open the camera");
//            }

            final double actualFramesPerSec = grabber.getFrameRate();
            log.info("actual frame rate={}", actualFramesPerSec);
            final boolean dropFrames = actualFramesPerSec > getConfig().getFramesPerSec();
            long lastTimestamp = 0;

            // Initialize capture preview window.
            final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / 2.2);

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

                OpenCVFrameConverter.ToIplImage convertToIplImage = new OpenCVFrameConverter.ToIplImage();

                int frameNumber = 0;
                int ssrc = new Random().nextInt();
                Frame capturedFrame;

//                OpenCVFrameConverter.ToIplImage converterToImage = new OpenCVFrameConverter.ToIplImage();
                while ((capturedFrame = grabber.grabImage()) != null) {
//                    Mat mat = converterToMat.convert(capturedFrame);

//                    log.info("Matrix dimensions: {}, {}", mat.arrayWidth(), mat.arrayHeight());

                    IplImage img = convertToIplImage.convert(capturedFrame);

                    CvRect rect = new CvRect();
                    rect.x(0);
                    rect.y(1000);
                    rect.width(1000);
                    rect.height(1000);

                    cvSetImageROI(img, rect);
                    IplImage cropped = cvCreateImage(cvGetSize(img), img.depth(), img.nChannels());
                    // Copy original image (only ROI) to the cropped image
                    cvCopy(img, cropped);
                    cvSaveImage("cropped.png", cropped);

                    Frame croppedFrame = convertToIplImage.convert(cropped);
                    Mat mat = converterToMat.convertToMat(croppedFrame);




//                    System.out.println("*********************** size:" + );
//                    capturedFrame = converterToMat.convert(mat);
                    long timestamp = System.currentTimeMillis();
                    // drop frames to adjust speed of camera recorder
                    if (dropFrames && timestamp - lastTimestamp < 1000 / getConfig().getFramesPerSec()) {
                        log.debug("Dropping captured frame to maintain desired frames per second");
                        continue;
                    }

                    log.info("frameNumber={}, timestamp={}, capturedFrame={}", frameNumber, timestamp, capturedFrame);

                    // Convert captured frame to PNG.
                    BytePointer jpgBytePointer = new BytePointer();
                    opencv_imgcodecs.imencode(".jpg", mat,  jpgBytePointer);
                    log.info("jpgBytePointer={}", jpgBytePointer);
                    byte[] pngByteArray = jpgBytePointer.getStringBytes();
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
                    log.info("Remaining: {}", jsonBytes.remaining());


                    // Write to Pravega.
                    CompletableFuture<Void> future = pravegaWriter.writeEvent(Integer.toString(videoFrame.camera), jsonBytes);

                    // Show our frame in the preview window..
                    if (cFrame.isVisible()) {
                        cFrame.showImage(croppedFrame);
                    }

                    // Make sure frame has been durably persisted to Pravega.
//                    future.get();

                    frameNumber++;
                    lastTimestamp = timestamp;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
