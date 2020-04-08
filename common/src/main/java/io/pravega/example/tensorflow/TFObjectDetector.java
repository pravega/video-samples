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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.*;
import org.tensorflow.framework.ConfigProto;
import org.tensorflow.framework.GPUOptions;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.List;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class TFObjectDetector implements Serializable {
    private final Logger log = LoggerFactory.getLogger(TFObjectDetector.class);

    // Params used for image processing
    private static final int IMAGE_DIMENSION = 416;
    private static final float SCALE = 255f;
    private static final String JPEG_BYTES_PLACEHOLDER_NAME = "image";

    private static TFObjectDetector single_instance;

    private final Session session;
    private final Output<Float> imagePreprocessingOutput;
    private final List<String> LABEL_DEF;
    private final ConfigProto config;

    public static TFObjectDetector getInstance() {
        // TODO: fix race condition
        if (single_instance == null) {
            single_instance = new TFObjectDetector();
        }

        return single_instance;
    }

    public TFObjectDetector() {
        log.info("@@@@@@@@@@@  new TF @@@@@@@@@@@  " );

        config = ConfigProto.newBuilder()
                .setGpuOptions(GPUOptions.newBuilder()
                        .setAllowGrowth(true)
                        .setPerProcessGpuMemoryFraction(0.04)
                        .build()
                ).build();

        InputStream graphFile = getClass().getResourceAsStream("/tiny-yolo-voc.pb");       // The model
        InputStream labelFile = getClass().getResourceAsStream("/yolo-voc-labels.txt");    // labels for classes used to train model

        byte[] GRAPH_DEF = IOUtil.readAllBytesOrExit(graphFile);
        LABEL_DEF = IOUtil.readAllLinesOrExit(labelFile);
        Graph graph = new Graph();
        graph.importGraphDef(GRAPH_DEF);
        session = new Session(graph, config.toByteArray());
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

    /**
     * Detect objects on the given image
     *
     * @param image the location of the image
     * @return output image with objects detected
     */
    public byte[] detect(byte[] image) {
        final long start = System.currentTimeMillis();
        final float[] tensorFlowOutput = executeYOLOGraph(image);
        final List<Recognition> recognitions = YOLOClassifier.getInstance().classifyImage(tensorFlowOutput, LABEL_DEF);
        final byte[] finalData = ImageUtil.getInstance().labelImage(image, recognitions);
        final long end = System.currentTimeMillis();
        log.info("@@@@@@@@@@@  TENSORFLOW  TIME TAKEN FOR DETECTION @@@@@@@@@@@  " + (end - start));
        return finalData;
    }

    /**
     * Executes graph on the given preprocessed image
     *
     * @param jpegBytes JPEG image
     * @return output tensor returned by tensorFlow
     */
    private float[] executeYOLOGraph(byte[] jpegBytes) {
        // Preprocess image (decode JPEG and resize)
        try (final Tensor<?> jpegTensor = Tensor.create(jpegBytes)) {
            final List<Tensor<?>> imagePreprocessorOutputs = session
                    .runner()
                    .feed(JPEG_BYTES_PLACEHOLDER_NAME, jpegTensor)
                    .fetch(imagePreprocessingOutput.op().name())
                    .run();
            assert imagePreprocessorOutputs.size() == 1;
            try (final Tensor<Float> preprocessedImageTensor = imagePreprocessorOutputs.get(0).expect(Float.class)) {
                // YOLO object detection
                final List<Tensor<?>> detectorOutputs = session
                        .runner()
                        .feed("input", preprocessedImageTensor)
                        .fetch("output")
                        .run();
                assert detectorOutputs.size() == 1;
                try (final Tensor<Float> resultTensor = detectorOutputs.get(0).expect(Float.class)) {
                    final float[] outputTensor = new float[YOLOClassifier.getInstance().getOutputSizeByShape(resultTensor)];
                    final FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
                    resultTensor.writeTo(floatBuffer);
                    return outputTensor;
                }
            }
        }
    }
}
