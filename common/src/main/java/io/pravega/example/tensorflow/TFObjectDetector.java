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

import java.io.InputStream;
import java.io.Serializable;
import java.nio.FloatBuffer;
import java.util.List;

/**
 * ObjectDetector class to detect objects using pre-trained models with TensorFlow Java API.
 */
public class TFObjectDetector implements Serializable {
    private final Logger log = LoggerFactory.getLogger(TFObjectDetector.class);

//    public List<Recognition> recognitions = null;
//    Graph graph;
    Session session;
    // Params used for image processing
    int SIZE = 416;
    float MEAN = 255f;
    Output<Float> output = null;
    private List<String> LABEL_DEF;
    private static TFObjectDetector single_instance = null;


    public static TFObjectDetector getInstance() {
        if(single_instance == null) {
            single_instance = new TFObjectDetector();
        }

        return single_instance;
    }

    public TFObjectDetector() {
        System.out.println("@@@@@@@@@@@  new TF @@@@@@@@@@@  " );

        InputStream graphFile = getClass().getResourceAsStream("/tiny-yolo-voc.pb");       // The model
        InputStream labelFile = getClass().getResourceAsStream("/yolo-voc-labels.txt");    // labels for classes used to train model

        byte[] GRAPH_DEF = IOUtil.readAllBytesOrExit(graphFile);
        LABEL_DEF = IOUtil.readAllLinesOrExit(labelFile);
        Graph graph = new Graph();
        graph.importGraphDef(GRAPH_DEF);
        session = new Session(graph);
        GraphBuilder graphBuilder = new GraphBuilder(graph);

        output =
                graphBuilder.div( // Divide each pixels with the MEAN
                        graphBuilder.resizeBilinear( // Resize using bilinear interpolation
                                graphBuilder.expandDims( // Increase the output tensors dimension
                                        graphBuilder.decodeJpeg(
                                                graph.opBuilder("Placeholder", "image")
                                                        .setAttr("dtype", DataType.STRING)
                                                        .build().output(0), 3),
                                        graphBuilder.constant("make_batch", 0)),
                                graphBuilder.constant("size", new int[]{SIZE, SIZE})),
                        graphBuilder.constant("scale", MEAN));
    }

    /**
     * Detect objects on the given image
     *
     * @param image the location of the image
     * @return output image with objects detected
     */
    public byte[] detect(byte[] image) {
        long start = System.currentTimeMillis();
        byte[] finalData = null;


        List<Recognition> recognitions = YOLOClassifier.getInstance().classifyImage(executeYOLOGraph(image), LABEL_DEF);

        finalData = ImageUtil.getInstance().labelImage(image, recognitions);

        long end = System.currentTimeMillis();
        System.out.println("@@@@@@@@@@@  TENSORFLOW  TIME TAKEN FOR DETECTION @@@@@@@@@@@  " + (end - start));


        return finalData;
    }

    /**
     * Executes graph on the given preprocessed image
     *
     * @param image preprocessed image
     * @return output tensor returned by tensorFlow
     */
    private float[] executeYOLOGraph(byte[] image) {
        long start = System.currentTimeMillis();

        float[] outputTensor;

        try(Tensor<Float> imageTensor = session.runner().feed("image", Tensor.create(image)).fetch(output.op().name()).run().get(0).expect(Float.class);
            Tensor<Float> result = session.runner().feed("input", imageTensor).fetch("output").run().get(0).expect(Float.class)) {
            long start1 = System.currentTimeMillis();
//        Tensor<Float> result = imageTensor;
                        long end1 = System.currentTimeMillis();
            System.out.println("@@@@@@@@@@@  result TIME TAKEN FOR DETECTION @@@@@@@@@@@  " + (end1 - start1));


            outputTensor = new float[YOLOClassifier.getInstance().getOutputSizeByShape(result)];
            FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
            result.writeTo(floatBuffer);

            long end = System.currentTimeMillis();
            System.out.println("@@@@@@@@@@@  executeYOLOGraph  TIME TAKEN FOR DETECTION @@@@@@@@@@@  " + (end - start));
        }

        return outputTensor;
    }

//    public List<Recognition> getRecognitions() {
//        return recognitions;
//    }

//    private static void printToConsole() {
//        for (Recognition recognition : getRecognitions()) {
//            System.out.println("TITLE :  " + recognition.getTitle());
//            System.out.println("CONFIDENCE :  " + recognition.getConfidence());
//        }
//    }

//    private void setRecognitions(List<Recognition> recs) {
//        this.recognitions = recs;
//    }

}