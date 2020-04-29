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

import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.FaceDetector;
import io.pravega.example.tensorflow.FaceRecognizer;
import io.pravega.example.tensorflow.TFObjectDetector;
import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.Mat;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

public class ObjectDetectorTests {
    private static Logger log = LoggerFactory.getLogger(ObjectDetectorTests.class);

    @Test
    @Ignore
    public void Test1() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("test_truck.jpg")).toPath());
        byte[] outBytes = new TFObjectDetector().detect(inBytes).getJpegBytes();
        Files.write((new File("detected_test_truck.jpg")).toPath(), outBytes);
    }

    @Test
    @Ignore
    public void Test2() throws Exception {
        InputStream origImage = getClass().getResourceAsStream("/thejas.jpg");       // The model
        InputStream otherImage = getClass().getResourceAsStream("/theja1.jpg");       // The model

        VideoFrame origFrame = new VideoFrame();
        origFrame.data = IOUtils.toByteArray(origImage);
        FaceDetector.getInstance().detectFaces(origFrame);
        FaceRecognizer.getInstance().recognizeFaces(origFrame);
        System.out.println(origFrame);
//
//        VideoFrame otherFrame = new VideoFrame();
//        otherFrame.data = IOUtils.toByteArray(otherImage);
//        FaceDetector.getInstance().detectFaces(otherFrame);
//        FaceRecognizer.getInstance().recognizeFaces(otherFrame);
//        System.out.println(otherFrame);
//
//        if(origFrame.embeddings.get(0).equals(otherFrame.embeddings.get(0))) {
//            System.out.println("It matched");
//        } else {
//            System.out.println("It did not match");
//        }
    }
}