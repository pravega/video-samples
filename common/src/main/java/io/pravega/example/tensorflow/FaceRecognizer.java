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


import io.pravega.example.common.Embedding;
import io.pravega.example.common.VideoFrame;
import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_objdetect.CascadeClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.*;
import org.tensorflow.framework.ConfigProto;
import org.tensorflow.framework.GPUOptions;

import java.io.*;
import java.net.URL;
import java.nio.FloatBuffer;
import java.util.*;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

/**
 * ObjectDetector class to detect and recognize faces using pre-trained models with TensorFlow Java API.
 */
public class FaceRecognizer implements Serializable, Closeable {
    // Params used for image processing
    private static final int IMAGE_DIMENSION = 160;
    private static final float SCALE = 255f;
    private static final String JPEG_BYTES_PLACEHOLDER_NAME = "image";
    private static final double THRESHOLD = 0.97;

    private final Logger log = LoggerFactory.getLogger(FaceRecognizer.class);
    private final Session session;
    private final Output<Float> imagePreprocessingOutput;
    private final ImageUtil imageUtil;

    private final File file;

    public FaceRecognizer() throws IOException {
        log.info("FaceRecognizer: initializing TensorFlow");
        final long t0 = System.currentTimeMillis();
        InputStream graphFile = FaceRecognizer.class.getResourceAsStream("/facenet.pb");       // Pre-trained model
	
	final ConfigProto config = ConfigProto.newBuilder()
                .setGpuOptions(GPUOptions.newBuilder()
                        .setAllowGrowth(true)
                        .setPerProcessGpuMemoryFraction(1.0/16.0)
                        .build()
                ).build();

        byte[] GRAPH_DEF = IOUtil.readAllBytesOrExit(graphFile);
        Graph graph = new Graph();
        graph.importGraphDef(GRAPH_DEF);
        session = new Session(graph, config.toByteArray());
        GraphBuilder graphBuilder = new GraphBuilder(graph);

        // Pipeline for JPEG decoding
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
        imageUtil = new ImageUtil();
        log.info("FaceRecognizer: done initializing TensorFlow; duration = {} ms", System.currentTimeMillis() - t0);


        // import face detection model
        String resource = "/haarcascade_frontalface_alt.xml";
        URL classifier = getClass().getResource(resource);

        // Accesses the jar file to get resources
        if (classifier.getProtocol().equals("jar")) {
            InputStream input = FaceRecognizer.class.getResourceAsStream(resource);
            file = File.createTempFile("haarcascase_frontalface_alt", ".tmp");
            OutputStream out = new FileOutputStream(file);
            int read;
            byte[] bytes = new byte[1024];

            while ((read = input.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            out.close();
            file.deleteOnExit();
        } else {
            //this will probably work in your IDE, but not from a JAR
            file = new File(classifier.getFile());
            file.deleteOnExit();
        }

        if (!file.exists()) {
            throw new RuntimeException("Error: File " + file + " not found!");
        }

    }

    public void warmup() throws Exception {
        log.info("warmup: BEGIN");
        final long t0 = System.currentTimeMillis();
        InputStream image = FaceRecognizer.class.getResourceAsStream("/ben_afflek_input_2.jpg");
        byte[] data = IOUtils.toByteArray(image);
        locateFaces(data);
        log.info("warmup: END; duration={} ms", System.currentTimeMillis() - t0);
    }

    @Override
    public void close() {
        session.close();
    }

    /**
     *
     * @param badgeId identifier for the face
     * @param faceLocation location of the face
     * @return a label representing location and identifier for face
     */
    public Recognition getLabel(String badgeId, BoundingBox faceLocation) {
        Recognition recognition = new Recognition(1, badgeId, (float) 1,
                new BoxPosition((float) (faceLocation.getX()),
                        (float) (faceLocation.getY()),
                        (float) (faceLocation.getWidth()),
                        (float) (faceLocation.getHeight())));

        return recognition;
    }

    /**
     * Extract the embeddings for the current face
     *
     * @param face in JPG format used to extract the embeddings
     * @return embeddings in a float array
     */
    public float[] embeddFace(byte[] face) {
        return executeGraph(face);
    }

    /**
     * Runs pre-trained facenet model to extract the embeddings from the image
     *
     * @param jpegBytes byte array to run the facenet model on
     * @return embeddings extracted by running the pre-trained model
     */
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
                 final Tensor<Boolean> preprocessedPhaseTrainTensor = Tensor.create(false).expect(Boolean.class)) {
                final List<Tensor<?>> detectorOutputs = session
                        .runner()
                        .feed("input", preprocessedInputTensor)
                        .feed("phase_train", preprocessedPhaseTrainTensor)
                        .fetch("embeddings")
                        .run();
                try (final Tensor<Float> resultTensor = detectorOutputs.get(0).expect(Float.class)) {
                    final float[] outputTensor = new float[(int) resultTensor.shape()[1]];
                    final FloatBuffer floatBuffer = FloatBuffer.wrap(outputTensor);
                    resultTensor.writeTo(floatBuffer);
                    return outputTensor;
                }
            }
        }
    }

    /**
     * @param cropBox  The location in the image to crop
     * @param faceData The data of the image in jpeg format
     * @return data of the image isolated to the cropped area
     */
    public byte[] cropFace(BoundingBox cropBox, Mat faceData) {
        Rect cropArea = new Rect((int) cropBox.getX(), (int) cropBox.getY(), (int) cropBox.getWidth(), (int) cropBox.getHeight());
        byte[] outData = new byte[0];
        if (0 <= cropBox.getX()
                && 0 <= cropBox.getWidth()
                && cropBox.getX() + cropBox.getWidth() <= faceData.cols()
                && 0 <= cropBox.getY()
                && 0 <= cropBox.getHeight()
                && cropBox.getY() + cropBox.getHeight() <= faceData.rows()) {

            // box within the image plane
            Mat croppedImage = new Mat(faceData, cropArea);
            outData = new byte[(int) (croppedImage.total() * croppedImage.elemSize())];
            imencode(".jpg", croppedImage, outData);
        }

        return outData;
    }

    /**
     * @param otherEmbedding     The current facial embedding found in the image
     * @param embeddingsDatabase The database of facial embeddings to compare to
     * @return The name of the person the embedding matches with in the embeddings database
     */
    public String matchEmbedding(float[] otherEmbedding, Iterator<Map.Entry<String, Embedding>> embeddingsDatabase, Set<String> badgeList) {
        String match = "Unknown";
        double minDiff = 1.0;

        while (embeddingsDatabase.hasNext()) {
            Map.Entry<String, Embedding> embeddingEntry = embeddingsDatabase.next();
            String personId = embeddingEntry.getKey();
            Embedding embedding = embeddingEntry.getValue();

            // only match with badges scanned
            if(badgeList != null && !badgeList.contains(personId)) {
                log.info("badge list has something, so skip this comparison");
                continue;
            }

            double diff = compareEmbeddings(embedding.embeddingValue, otherEmbedding);
//            log.info("distance with " + personId + " is " + diff);

            // Matches if within threshold
            if (diff < THRESHOLD && diff < minDiff) {
                match = personId;
            }
        }

        return match;
    }

    /**
     * @param origEmbedding  The data for a facial embedding
     * @param otherEmbedding The data for another facial embedding
     * @return The difference between the facial embeddings
     */
    public double compareEmbeddings(float[] origEmbedding, float[] otherEmbedding) {
        double sumDiffSq = 0;

        for (int i = 0; i < origEmbedding.length; i++) {
            sumDiffSq += Math.pow(origEmbedding[i] - otherEmbedding[i], 2);
        }

        return Math.sqrt(sumDiffSq);
    }

    /**
     * @param frameData data that represents the image with faces
     * @return location of the faces
     * @throws Exception
     */
    public List<BoundingBox> locateFaces(byte[] frameData) throws Exception {
        try (Mat imageMat = imdecode(new Mat(frameData), IMREAD_UNCHANGED);
             CvArr inputImage = new IplImage(imageMat);
             RectVector faces = new RectVector();

             CvArr grayImage = cvCreateImage(cvGetSize(inputImage), 8, 1); //converting image to grayscale

             CascadeClassifier faceCascade = new CascadeClassifier();


             Size size = new Size();
             ) {



            cvCvtColor(inputImage, grayImage, COLOR_BGR2GRAY); // Convert image to grayscale
            cvEqualizeHist(grayImage, grayImage);

            String classifierPath = file.getAbsolutePath();
            boolean modelLoaded = faceCascade.load(classifierPath);
//            log.info("facial detection model load: " + modelLoaded);

            int absoluteFaceSize = 0;
            int height = grayImage.arrayHeight();
            if (Math.round(height * 0.2f) > 0) {
                absoluteFaceSize = Math.round(height * 0.2f);
            }

            Size faceSize = size.height(absoluteFaceSize);
            faceSize = faceSize.width(absoluteFaceSize);

            // Identify location of the faces
            faceCascade.detectMultiScale(cvarrToMat(grayImage), faces, 1.1, 2, CASCADE_SCALE_IMAGE, faceSize, size);

            List<BoundingBox> recognizedBoxes = new ArrayList<>();

            for (int i = 0; i < faces.size(); i++) {
                double boxX = Math.max(faces.get(i).x() - 10, 0);
                double boxY = Math.max(faces.get(i).y() - 10, 0);
                double boxWidth = Math.min(faces.get(i).width() + 20, imageMat.arrayWidth() - boxX);
                double boxHeight = Math.min(faces.get(i).height() + 20, imageMat.arrayHeight() - boxY);
                double boxConfidence = -1.0;
                double[] boxClasses = new double[1];

                BoundingBox currentBox = new BoundingBox(boxX, boxY, boxWidth, boxHeight, boxConfidence, boxClasses);

                recognizedBoxes.add(currentBox);
            }

            faceSize.close();

            return recognizedBoxes;
        } catch (Exception e) {
            throw new Exception(e);
        } finally {

        }
    }
}
