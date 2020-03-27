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
package io.pravega.example.tensorflow;


import io.pravega.example.common.VideoFrame;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_objdetect.CascadeClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class FaceDetector implements Serializable {
    private static FaceDetector single_instance;
    private final Logger log = LoggerFactory.getLogger(FaceDetector.class);

    public FaceDetector() {
    }

    public static FaceDetector getInstance() {
        // TODO: fix race condition
        if (single_instance == null) {
            single_instance = new FaceDetector();
        }

        return single_instance;
    }

    /**
     * Detect faces on the given frame
     *
     * @param videoFrame the video frame which is being processed
     * @return output video frame with faces detected
     */
    public VideoFrame detectFaces(VideoFrame videoFrame) throws IOException {
        Mat imageMat = imdecode(new Mat(videoFrame.data), IMREAD_UNCHANGED);
        CvArr inputImage = new IplImage(imageMat);

        CvArr grayImage = cvCreateImage(cvGetSize(inputImage), 8, 1); //converting image to grayscale

        cvCvtColor(inputImage, grayImage, COLOR_BGR2GRAY); // Convert image to grayscale
        cvEqualizeHist(grayImage, grayImage);

        String classifierPath = "./camera-recorder/src/main/resources/haarcascade_frontalface_alt.xml"; // face detection model configuration
        CascadeClassifier faceCascade = new CascadeClassifier();
        faceCascade.load(classifierPath);

        RectVector faces = new RectVector();
        int absoluteFaceSize = 0;
        int height = grayImage.arrayHeight();
        if (Math.round(height * 0.2f) > 0) {
            absoluteFaceSize = Math.round(height * 0.2f);
        }

        // perform face detection
        faceCascade.detectMultiScale(cvarrToMat(grayImage), faces, 1.1, 2, 0 | CASCADE_SCALE_IMAGE, new Size(absoluteFaceSize, absoluteFaceSize), new Size());

//        BufferedImage bufferedImage = ImageIO.read(imageStream);
        ByteArrayInputStream bais = new ByteArrayInputStream(videoFrame.data);
        BufferedImage bufferedImage = ImageIO.read(bais);

        Graphics2D graphics = (Graphics2D) bufferedImage.getGraphics();
        graphics.setColor(Color.green);

        for (int i = 0; i < faces.size(); i++) {
            graphics.drawRect(faces.get(i).x(), faces.get(i).y(), faces.get(i).width(), faces.get(i).height());

            double boxX = faces.get(i).x();
            double boxY = faces.get(i).y();
            double boxWidth = faces.get(i).width();
            double boxHeight = faces.get(i).height();
            double boxConfidence = -1;
            double[] boxClasses = new double[0];

            BoundingBox currentBox = new BoundingBox(boxX, boxY, boxWidth, boxHeight, boxConfidence, boxClasses);

            videoFrame.recognizedBoxes.add(currentBox);
        }

        graphics.dispose();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "jpg", baos);
        videoFrame.data = baos.toByteArray();

        File outputfile = new File("./camera-recorder/src/main/resources/detected_face.jpg");
        ImageIO.write(bufferedImage, "jpg", outputfile);

        return videoFrame;
    }
}
