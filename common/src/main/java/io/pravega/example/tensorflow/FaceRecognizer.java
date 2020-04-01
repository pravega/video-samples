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
import org.bytedeco.opencv.opencv_core.CvArr;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Rect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.*;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_imgcodecs.*;

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

    public VideoFrame recognizeFaces(VideoFrame frame) {
        Mat imageMat = imdecode(new Mat(frame.data), IMREAD_UNCHANGED);

        for(BoundingBox currentFace: frame.recognizedBoxes) {
            byte[] croppedFace = cropFace(currentFace, imageMat);
            embeddFace(croppedFace);
        }

        return frame;
    }

    private void embeddFace(byte[] image) {
        final float[] embeddings = executeGraph(image);



//        final List<Recognition> recognitions = YOLOClassifier.getInstance().classifyImage(tensorFlowOutput, LABEL_DEF);
//        final byte[] finalData = ImageUtil.getInstance().labelImage(image, recognitions);
    }

    private float[] executeGraph(byte[] jpegBytes) {
        // Preprocess image (decode JPEG and resize)
        try (final Tensor<?> jpegTensor = Tensor.create(jpegBytes)) {
            final List<Tensor<?>> imagePreprocessorOutputs = session
                    .runner()
                    .feed(JPEG_BYTES_PLACEHOLDER_NAME, jpegTensor)
                    .fetch(imagePreprocessingOutput.op().name())
                    .run();
            assert imagePreprocessorOutputs.size() == 1;

            try (final Tensor<Float> preprocessedInputTensor = imagePreprocessorOutputs.get(0).expect(Float.class);
                final Tensor<Boolean> preprocessedPhaseTrainTensor = Tensor.create(true).expect(Boolean.class)) {
                // YOLO object detection
                final List<Tensor<?>> detectorOutputs = session
                        .runner()
                        .feed("input", preprocessedInputTensor)
                        .feed("phase_train", preprocessedInputTensor)
                        .fetch("embeddings")
                        .run();
                assert detectorOutputs.size() == 1;
                try (final Tensor<Float> resultTensor = detectorOutputs.get(0).expect(Float.class)) {
                    System.out.println(resultTensor.shape());
                    final float[] outputTensor = new float[(int)resultTensor.shape()[0]];
                    final FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
                    resultTensor.writeTo(floatBuffer);
                    return outputTensor;
                }
            }
        }
    }


    private byte[] cropFace(BoundingBox cropBox, Mat faceData) {
        Rect cropArea = new Rect((int) cropBox.getX(), (int) cropBox.getY(), (int) cropBox.getWidth(), (int) cropBox.getHeight());
        Mat croppedImage = new Mat(faceData, cropArea);
        byte[] outData = new byte[(int)(croppedImage.total()*croppedImage.elemSize())];
        imencode(".jpg", croppedImage, outData);

        return outData;
    }
}
