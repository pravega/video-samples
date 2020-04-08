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
import scala.concurrent.java8.FuturesConvertersImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_core.cvarrToMat;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class FaceRecognizer implements Serializable {
    private final Logger log = LoggerFactory.getLogger(FaceRecognizer.class);

    // Params used for image processing
    private static final int IMAGE_DIMENSION = 160;
    private static final float SCALE = 255f;
    private static final String JPEG_BYTES_PLACEHOLDER_NAME = "image";

    private static FaceRecognizer single_instance;

    private final Session session;
    private final Output<Float> imagePreprocessingOutput;


    public static FaceRecognizer getInstance() {
        // TODO: fix race condition
        if (single_instance == null) {
            single_instance = new FaceRecognizer();
        }

        return single_instance;
    }

    public FaceRecognizer() {

        InputStream graphFile = getClass().getResourceAsStream("/facenet.pb");       // The model

        byte[] GRAPH_DEF = IOUtil.readAllBytesOrExit(graphFile);
        Graph graph = new Graph();
        graph.importGraphDef(GRAPH_DEF);
        session = new Session(graph);
        GraphBuilder graphBuilder = new GraphBuilder(graph);

        imagePreprocessingOutput =
                graphBuilder.div( // Divide each pixels with the MEAN
                        graphBuilder.resizeBilinear( // Resize using bilinear interpolation
                                graphBuilder.expandDims( // Increase the output tensors dimension
                                        graphBuilder.decodeJpeg(
                                                graph.opBuilder("Placeholder", JPEG_BYTES_PLACEHOLDER_NAME)
                                                        .setAttr("dtype", DataType.STRING)
                                                        .build().output(0), 3),
                                        graphBuilder.constant("make_batch", 0)),
                                graphBuilder.constant("size", new int[]{IMAGE_DIMENSION, IMAGE_DIMENSION})),
                        graphBuilder.constant("scale", SCALE));
    }

    public void recognizeFaces(VideoFrame frame) throws IOException {
        frame.recognizedBoxes = this.detectFaces(frame.data);

        Mat imageMat = imdecode(new Mat(frame.data), IMREAD_UNCHANGED);

        for(int i=0; i< frame.recognizedBoxes.size(); i++){
            BoundingBox currentFace = frame.recognizedBoxes.get(i);
            byte[] croppedFace = cropFace(currentFace, imageMat);
            frame.embeddings.add(embeddFace(croppedFace));

            String match = matchEmbedding(frame.embeddings.get(i));

            if(match != "") {
                Recognition recognition = new Recognition(1, match, (float)1,
                        new BoxPosition((float) (currentFace.getX()),
                                (float) (currentFace.getY()),
                                (float) (currentFace.getWidth()),
                                (float) (currentFace.getHeight())));
//                log.info("leftInt =" + recognition.getLocation().getLeftInt());

                frame.data= ImageUtil.getInstance().labelFace(frame.data, recognition);
            }
        }
    }

    public float[] embeddFace(byte[] image) {
        final float[] embeddings = executeGraph(image);


        return embeddings;
//        final List<Recognition> recognitions = YOLOClassifier.getInstance().classifyImage(tensorFlowOutput, LABEL_DEF);
//        final byte[] finalData = ImageUtil.getInstance().labelImage(image, recognitions);
    }

    public float[] executeGraph(byte[] jpegBytes) {
        // Preprocess image (decode JPEG and resize)
        try (final Tensor<?> jpegTensor = Tensor.create(jpegBytes)) {
            final List<Tensor<?>> imagePreprocessorOutputs = session
                    .runner()
                    .feed(JPEG_BYTES_PLACEHOLDER_NAME, jpegTensor)
                    .fetch(imagePreprocessingOutput.op().name())
                    .run();
            assert imagePreprocessorOutputs.size() == 1;

            try (final Tensor<Float> preprocessedInputTensor = imagePreprocessorOutputs.get(0).expect(Float.class);
                final Tensor<Boolean> preprocessedPhaseTrainTensor = Tensor.create(false).expect(Boolean.class)) {
                final List<Tensor<?>> detectorOutputs = session
                        .runner()
                        .feed("input", preprocessedInputTensor)
                        .feed("phase_train", preprocessedPhaseTrainTensor)
                        .fetch("embeddings")
                        .run();
//                assert detectorOutputs.size() == 1;
                try (final Tensor<Float> resultTensor = detectorOutputs.get(0).expect(Float.class)) {
                    final float[] outputTensor = new float[(int)resultTensor.shape()[1]];
                    final FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
                    resultTensor.writeTo(floatBuffer);
                    return outputTensor;
                }
            }
        }
    }


    public byte[] cropFace(BoundingBox cropBox, Mat faceData) {
        Rect cropArea = new Rect((int) cropBox.getX(), (int) cropBox.getY(), (int) cropBox.getWidth(), (int) cropBox.getHeight());
        Mat croppedImage = new Mat(faceData, cropArea);
        byte[] outData = new byte[(int)(croppedImage.total()*croppedImage.elemSize())];
        imencode(".jpg", croppedImage, outData);

        return outData;
    }

    public String matchEmbedding(float[] otherEmbedding) {
        try{
            VideoFrame origFrame = new VideoFrame();
            InputStream origImage = getClass().getResourceAsStream("/TJ_now.jpg");
            origFrame.data = IOUtils.toByteArray(origImage);
            FaceDetector.getInstance().detectFaces(origFrame);
            Mat imageMat = imdecode(new Mat(origFrame.data), IMREAD_UNCHANGED);
            origFrame.data = cropFace(origFrame.recognizedBoxes.get(0), imageMat);

            float[] origEmbedding = embeddFace(origFrame.data);

            double diff = compareEmbeddings(origEmbedding, otherEmbedding);

            log.info("distance is " + diff);

            String match = "";

            if(diff < 1.05) {
                match = "Thejas";
            }

            return match;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public double compareEmbeddings(float[] origEmbedding, float[] otherEmbedding) {
        double sumDiffSq = 0;

        for(int i=0; i< origEmbedding.length; i++) {
            sumDiffSq += Math.pow(origEmbedding[i] - otherEmbedding[i], 2);
        }

        return Math.sqrt(sumDiffSq);
    }

    public List<BoundingBox> detectFaces(byte[] imageBytes) throws IOException {
        Mat imageMat = imdecode(new Mat(imageBytes), IMREAD_UNCHANGED);
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

        System.out.println(cvarrToMat(grayImage));

        // perform face detection
        faceCascade.detectMultiScale(cvarrToMat(grayImage), faces, 1.1, 2, 0 | CASCADE_SCALE_IMAGE, new Size(absoluteFaceSize, absoluteFaceSize), new Size());

        List<BoundingBox> recognizedBoxes = new ArrayList<BoundingBox>();

        for (int i = 0; i < faces.size(); i++) {
            double boxX = Math.max(faces.get(i).x()-10,0);
            double boxY = Math.max(faces.get(i).y()-10,0);
            double boxWidth = Math.min(faces.get(i).width()+20,imageMat.arrayWidth());
            double boxHeight = Math.min(faces.get(i).height()+20,imageMat.arrayHeight());
            double boxConfidence = -1.0;
            double[] boxClasses = new double[1];

            BoundingBox currentBox = new BoundingBox(boxX, boxY, boxWidth, boxHeight, boxConfidence, boxClasses);

            recognizedBoxes.add(currentBox);
        }

        return recognizedBoxes;
    }
}
