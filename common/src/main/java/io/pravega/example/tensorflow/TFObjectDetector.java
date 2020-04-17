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
import org.tensorflow.DataType;
import org.tensorflow.Graph;
import org.tensorflow.Output;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.framework.ConfigProto;
import org.tensorflow.framework.GPUOptions;

import java.io.Closeable;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.List;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class TFObjectDetector implements Serializable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(TFObjectDetector.class);

    // Params used for image processing
    private static final int IMAGE_DIMENSION = 416;
    private static final float SCALE = 255f;
    private static final String JPEG_BYTES_PLACEHOLDER_NAME = "image";

    private final Session session;
    private final Output<Float> imagePreprocessingOutput;
    private final List<String> LABEL_DEF;
    private final YOLOClassifier yoloClassifier;
    private final ImageUtil imageUtil;

    public TFObjectDetector() {
        log.info("TFObjectDetector: initializing TensorFlow");
        final long t0 = System.currentTimeMillis();
        final ConfigProto config = ConfigProto.newBuilder()
                .setGpuOptions(GPUOptions.newBuilder()
                        .setAllowGrowth(true)
                        .setPerProcessGpuMemoryFraction(1.0/16.0)
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

        yoloClassifier = new YOLOClassifier();
        imageUtil = new ImageUtil();
        log.info("TFObjectDetector: done initializing TensorFlow; duration = {} ms", System.currentTimeMillis() - t0);
    }

    public void warmup() {
        log.info("warmup: BEGIN");
        final long t0 = System.currentTimeMillis();
        detect(imageUtil.createBlankJpeg(1280, 720));
        log.info("warmup: END; duration={} ms", System.currentTimeMillis() - t0);
    }

    @Override
    public void close() {
        session.close();
    }

    /**
     * Detect objects on the given image
     *
     * @param image the location of the image
     * @return output image with objects detected
     */
    public DetectionResult detect(byte[] image) {
        log.info("detect: BEGIN");
        final long t0 = System.currentTimeMillis();
        final float[] tensorFlowOutput = executeYOLOGraph(image);
        final List<Recognition> recognitions = yoloClassifier.classifyImage(tensorFlowOutput, LABEL_DEF);
        final byte[] jpegBytes = imageUtil.labelImage(image, recognitions);
        final long dt = System.currentTimeMillis() - t0;
        final double fps = 1000.0/dt;
        log.info("detect: elapsed time for single frame: {} ms ({} fps)", dt, fps);
        log.info("detect: END");
        return new DetectionResult(recognitions, jpegBytes);
    }

    /**
     * Executes graph on the given preprocessed image
     *
     * @param jpegBytes JPEG image
     * @return output tensor returned by tensorFlow
     */
    private float[] executeYOLOGraph(byte[] jpegBytes) {
        // Preprocess image (decode JPEG and resize)
        log.info("executeYOLOGraph: BEGIN");
        try (final Tensor<?> jpegTensor = Tensor.create(jpegBytes)) {
            final List<Tensor<?>> imagePreprocessorOutputs = session
                    .runner()
                    .feed(JPEG_BYTES_PLACEHOLDER_NAME, jpegTensor)
                    .fetch(imagePreprocessingOutput.op().name())
                    .run();
            log.info("executeYOLOGraph: got imagePreprocessorOutputs");
            assert imagePreprocessorOutputs.size() == 1;
            try (final Tensor<Float> preprocessedImageTensor = imagePreprocessorOutputs.get(0).expect(Float.class)) {
                // YOLO object detection
                final List<Tensor<?>> detectorOutputs = session
                        .runner()
                        .feed("input", preprocessedImageTensor)
                        .fetch("output")
                        .run();
                log.info("executeYOLOGraph: got detectorOutputs");
                assert detectorOutputs.size() == 1;
                try (final Tensor<Float> resultTensor = detectorOutputs.get(0).expect(Float.class)) {
                    final float[] outputTensor = new float[yoloClassifier.getOutputSizeByShape(resultTensor)];
                    final FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
                    resultTensor.writeTo(floatBuffer);
                    return outputTensor;
                }
            }
        } finally {
            log.info("executeYOLOGraph: END");
        }
    }

    static public class DetectionResult {
        private final List<Recognition> recognitions;
        private final byte[] jpegBytes;

        public DetectionResult(List<Recognition> recognitions, byte[] jpegBytes) {
            this.recognitions = recognitions;
            this.jpegBytes = jpegBytes;
        }

        public List<Recognition> getRecognitions() {
            return recognitions;
        }

        public byte[] getJpegBytes() {
            return jpegBytes;
        }
    }
}
