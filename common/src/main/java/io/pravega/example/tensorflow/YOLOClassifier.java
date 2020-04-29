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

import org.apache.commons.math3.analysis.function.Sigmoid;
import org.tensorflow.Tensor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


public class YOLOClassifier {
    private final static float OVERLAP_THRESHOLD = 0.5f;
    private final static double anchors[] = {1.08, 1.19, 3.42, 4.41, 6.63, 11.38, 9.42, 5.11, 16.62, 10.52};
    private final static int NUMBER_OF_BOXES = 13;
    private final static int MAX_RECOGNIZED_CLASSES = 24;
    private final static float THRESHOLD = 0.15f;   // Change to 0.10 to reduce detected objects.
    private final static int MAX_RESULTS = 24;
    private final static int NUMBER_OF_BOUNDING_BOXES = 5;

    /**
     * Gets the number of classes based on the tensor shape
     *
     * @param result - the tensorflow output
     * @return the number of classes
     */
    public int getOutputSizeByShape(Tensor<Float> result) {
        return (int) (result.shape()[3] * (NUMBER_OF_BOXES * NUMBER_OF_BOXES));
    }

    /**
     * It classifies the object/objects on the image
     *
     * @param tensorFlowOutput output from the TensorFlow, it is a 13x13x((num_class +1) * 5) tensor
     *                         125 = (numClass +  Tx, Ty, Tw, Th, To) * 5 - cause we have 5 boxes per each cell
     * @param labels           a string vector with the labels
     * @return a list of recognition objects
     */
    public List<Recognition> classifyImage(final float[] tensorFlowOutput, final List<String> labels) {
        int numClass = (int) (tensorFlowOutput.length / (Math.pow(NUMBER_OF_BOXES, 2) * NUMBER_OF_BOUNDING_BOXES) - 5);
        BoundingBox[][][] boundingBoxPerCell = new BoundingBox[NUMBER_OF_BOXES][NUMBER_OF_BOXES][NUMBER_OF_BOUNDING_BOXES];
        PriorityQueue<Recognition> priorityQueue = new PriorityQueue(MAX_RECOGNIZED_CLASSES, new RecognitionComparator());

        int offset = 0;
        for (int cy = 0; cy < NUMBER_OF_BOXES; cy++) {        // SIZE * SIZE cells
            for (int cx = 0; cx < NUMBER_OF_BOXES; cx++) {
                for (int b = 0; b < NUMBER_OF_BOUNDING_BOXES; b++) {   // 5 bounding boxes per each cell
                    boundingBoxPerCell[cx][cy][b] = getBoundingBox(tensorFlowOutput, cx, cy, b, numClass, offset);
                    calculateTopPredictions(boundingBoxPerCell[cx][cy][b], priorityQueue, labels);
                    offset = offset + numClass + 5;
                }
            }
        }

        return getRecognition(priorityQueue);
    }

    private BoundingBox getBoundingBox(final float[] tensorFlowOutput, int cx, int cy, int b, int numClass, int offset) {
        Sigmoid sigmoid = new Sigmoid();
        double x = (cx + sigmoid.value(tensorFlowOutput[offset])) * 32;
        double y = (cy + sigmoid.value(tensorFlowOutput[offset + 1])) * 32;
        double width = Math.exp(tensorFlowOutput[offset + 2]) * anchors[2 * b] * 32;
        double height = Math.exp(tensorFlowOutput[offset + 3]) * anchors[2 * b + 1] * 32;
        double confidence = sigmoid.value(tensorFlowOutput[offset + 4]);
        double[] classes = new double[numClass];

        for (int probIndex = 0; probIndex < numClass; probIndex++) {
            classes[probIndex] = tensorFlowOutput[probIndex + offset + 5];
        }

        BoundingBox boundingBox = new BoundingBox(x, y, width, height, confidence, classes);

        return boundingBox;
    }


    private void calculateTopPredictions(final BoundingBox boundingBox, final PriorityQueue<Recognition> predictionQueue,
                                         final List<String> labels) {
        for (int i = 0; i < boundingBox.getClasses().length; i++) {
            ArgMax.Result argMax = new ArgMax(new SoftMax(boundingBox.getClasses()).getValue()).getResult();
            double confidenceInClass = argMax.getMaxValue() * boundingBox.getConfidence();
            if (confidenceInClass > THRESHOLD) {
                predictionQueue.add(new Recognition(argMax.getIndex(), labels.get(argMax.getIndex()), (float) confidenceInClass,
                        new BoxPosition((float) (boundingBox.getX() - boundingBox.getWidth() / 2),
                                (float) (boundingBox.getY() - boundingBox.getHeight() / 2),
                                (float) boundingBox.getWidth(),
                                (float) boundingBox.getHeight())));
            }
        }
    }

    private List<Recognition> getRecognition(final PriorityQueue<Recognition> priorityQueue) {
        List<Recognition> recognitions = new ArrayList();

        if (priorityQueue.size() > 0) {
            // Best recognition
            Recognition bestRecognition = priorityQueue.poll();
            recognitions.add(bestRecognition);

            for (int i = 0; i < Math.min(priorityQueue.size(), MAX_RESULTS); ++i) {
                Recognition recognition = priorityQueue.poll();
                boolean overlaps = false;
                for (Recognition previousRecognition : recognitions) {
                    overlaps = overlaps || (getIntersectionProportion(previousRecognition.getLocation(),
                            recognition.getLocation()) > OVERLAP_THRESHOLD);
                }

                if (!overlaps) {
                    recognitions.add(recognition);
                }
            }
        }

        return recognitions;
    }

    private float getIntersectionProportion(BoxPosition primaryShape, BoxPosition secondaryShape) {
        if (overlaps(primaryShape, secondaryShape)) {
            float intersectionSurface = Math.max(0, Math.min(primaryShape.getRight(), secondaryShape.getRight()) - Math.max(primaryShape.getLeft(), secondaryShape.getLeft())) *
                    Math.max(0, Math.min(primaryShape.getBottom(), secondaryShape.getBottom()) - Math.max(primaryShape.getTop(), secondaryShape.getTop()));

            float surfacePrimary = Math.abs(primaryShape.getRight() - primaryShape.getLeft()) * Math.abs(primaryShape.getBottom() - primaryShape.getTop());

            return intersectionSurface / surfacePrimary;
        }

        return 0f;

    }

    private boolean overlaps(BoxPosition primary, BoxPosition secondary) {
        return primary.getLeft() < secondary.getRight() && primary.getRight() > secondary.getLeft()
                && primary.getTop() < secondary.getBottom() && primary.getBottom() > secondary.getTop();
    }

    // Intentionally reversed to put high confidence at the head of the queue.
    private class RecognitionComparator implements Comparator<Recognition> {
        @Override
        public int compare(final Recognition recognition1, final Recognition recognition2) {
            return Float.compare(recognition2.getConfidence(), recognition1.getConfidence());
        }
    }
}
