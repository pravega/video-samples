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

import io.pravega.client.admin.StreamManager;
import io.pravega.example.common.PravegaUtil;
import io.pravega.example.tensorflow.QRCode;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.opencv.global.opencv_videoio;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_videoio.VideoCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.zxing.*;

import java.io.IOException;
import java.rmi.ConnectIOException;

import static org.bytedeco.opencv.global.opencv_imgcodecs.imencode;

/**
 * Reads video images from a web cam and writes them to a Pravega stream.
 */
public class Test implements Runnable {
    private static Logger log = LoggerFactory.getLogger(Test.class);

    private final QRCode qrcode;
    private final AppConfiguration config;

    public Test(AppConfiguration appConfiguration) {
        config = appConfiguration;
        qrcode = new QRCode();
    }

    public static void main(String... args) {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        Runnable app = new Test(config);
        app.run();
    }

    public AppConfiguration getConfig() {
        return config;
    }

    public void run() {
            // Initialize camera.
            final int captureWidth = getConfig().getImageWidth();
            final int captureHeight = getConfig().getImageHeight();
            log.info("creating grabber");
            final VideoCapture cap = new VideoCapture(getConfig().getCameraDeviceNumber());

            if (!cap.open(getConfig().getCameraDeviceNumber())) {
                try {
                    throw new ConnectIOException("Cannot open the camera");
                } catch (ConnectIOException e) {
                    e.printStackTrace();
                }
            }

            log.info("starting video capture");
            cap.set(opencv_videoio.CAP_PROP_FPS, getConfig().getFramesPerSec());
            cap.set(opencv_videoio.CAP_PROP_FRAME_WIDTH, captureWidth);
            cap.set(opencv_videoio.CAP_PROP_FRAME_HEIGHT, captureHeight);

            final double actualFramesPerSec = cap.get(opencv_videoio.CAP_PROP_FPS);
            log.info("actual frame rate={}", actualFramesPerSec);
            final boolean dropFrames = actualFramesPerSec > getConfig().getFramesPerSec();
            long lastTimestamp = 0;

            // Initialize capture preview window.
            final CanvasFrame cFrame = new CanvasFrame("Capture Preview", CanvasFrame.getDefaultGamma() / 2.2);

            // Create Pravega stream.
//            PravegaUtil.createStream(getConfig().getClientConfig(), getConfig().getOutputStreamConfig());


            Mat mat = new Mat();
            while (cap.read(mat)) {
                byte[] imageBytes = new byte[(int) (mat.total() * mat.elemSize())];
                imencode(".jpg", mat, imageBytes);

                String qrVal = null;
                try {
                    qrVal = qrcode.readQRCode(imageBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NotFoundException e) {
                    // skip if not found
                }

                log.info("reached");
                log.info("qr info is: " + qrVal);
            }
    }
}
