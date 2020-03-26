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
import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_objdetect.CascadeClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.*;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.FloatBuffer;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class FaceDetector implements Serializable {
    private final Logger log = LoggerFactory.getLogger(FaceDetector.class);

    // Params used for image processing
    private static final int IMAGE_DIMENSION = 416;
    private static final float SCALE = 255f;
    private static final String JPEG_BYTES_PLACEHOLDER_NAME = "image";

    private static FaceDetector single_instance;
//
//    private final Session session;
//    private final Output<Float> imagePreprocessingOutput;
//    private final List<String> LABEL_DEF;

    public static FaceDetector getInstance() {
        // TODO: fix race condition
        if (single_instance == null) {
            single_instance = new FaceDetector();
        }

        return single_instance;
    }

//    public FaceDetector() {
//
//    }

    /**
     * Detect faces on the given frame
     *
     * @param videoFrame the video frame which is being processed
     * @return output video frame with faces detected
     */
    public VideoFrame detectFaces(VideoFrame videoFrame) throws IOException
    {
//        InputStream imageStream = this.getClass().getResourceAsStream("/ben_afflek_input_1.jpg");
//        byte[] videoFrame = IOUtils.toByteArray(imageStream);
        Mat imageMat = imdecode(new Mat(videoFrame.data), IMREAD_UNCHANGED);
        CvArr inputImage = new IplImage(imageMat);

        CvArr grayImage = cvCreateImage(cvGetSize(inputImage), 8, 1); //converting image to grayscale

        cvCvtColor(inputImage, grayImage, COLOR_BGR2GRAY); // Convert image to grayscale
        cvEqualizeHist(grayImage, grayImage);

//        InputStream classifier = getClass().getResourceAsStream("/haarcascade_frontalface_alt.xml");
//        String classifierPath = Paths.get(Test.class.getResource("/haarcascade_frontalface_alt.xml").toURI()).toFile().getPath();
        String classifierPath = "./camera-recorder/src/main/resources/haarcascade_frontalface_alt.xml";
        CascadeClassifier faceCascade = new CascadeClassifier();
        faceCascade.load(classifierPath);

        RectVector faces = new RectVector();
        int absoluteFaceSize = 0;
        int height = grayImage.arrayHeight();
        if (Math.round(height * 0.2f) > 0)
        {
            absoluteFaceSize = Math.round(height * 0.2f);
        }

        faceCascade.detectMultiScale(cvarrToMat(grayImage), faces, 1.1, 2, 0 | CASCADE_SCALE_IMAGE, new Size(absoluteFaceSize, absoluteFaceSize), new Size());

//        BufferedImage bufferedImage = ImageIO.read(imageStream);
        ByteArrayInputStream bais = new ByteArrayInputStream(videoFrame.data);
        BufferedImage bufferedImage = ImageIO.read(bais);

        Graphics2D graphics = (Graphics2D) bufferedImage.getGraphics();
        graphics.setColor(Color.green);

        for (int i = 0; i < faces.size(); i++) {
            graphics.drawRect(faces.get(i).x(),faces.get(i).y(), faces.get(i).width(), faces.get(i).height());
        }

        graphics.dispose();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "jpg", baos);
        videoFrame.data = baos.toByteArray();

        File outputfile = new File("./camera-recorder/src/main/resources/detected_face.jpg");
        ImageIO.write(bufferedImage, "jpg", outputfile);

        videoFrame.recognizedBoxes = faces;

        return videoFrame;
    }
}
