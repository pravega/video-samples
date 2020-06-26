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

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.common.*;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.JsonDeserializationSchema;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

/**
 * This job matches scanned faces to known recorded faces for facial recognition by labelling recognized faces
 * and locating recognized faces.
 */
public class FlinkFaceRecognizerJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.class);

    public FlinkFaceRecognizerJob(VideoAppConfiguration config) {
        super(config);
    }

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        FlinkFaceRecognizerJob job = new FlinkFaceRecognizerJob(config);
        job.run();
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final String jobName = FlinkFaceRecognizerJob.class.getName();
            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getPersonDatabaseStreamConfig());
            createStream(getConfig().getOutputStreamConfig());

            //
            // Create video datastream.
            //

            final StreamCut startStreamCut = resolveStartStreamCut(getConfig().getInputStreamConfig());
            final StreamCut endStreamCut = resolveEndStreamCut(getConfig().getInputStreamConfig());

            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            final DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFrames
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<ChunkedVideoFrame>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(ChunkedVideoFrame element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarks")
                    .name("assignTimestampsAndWatermarks");

            // Unchunk (disabled).
            // Operator: ChunkedVideoFrameReassembler
            final DataStream<VideoFrame> videoFrames = inChunkedVideoFramesWithTimestamps
                    .map(VideoFrame::new)
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");

            //
            // Create person database transaction datastream.
            //
            final FlinkPravegaReader<Transaction> flinkPravegaReaderTransactions = FlinkPravegaReader.<Transaction>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getPersonDatabaseStreamConfig().getStream())
                    .withDeserializationSchema(new JsonDeserializationSchema<>(Transaction.class))
                    .build();
            final DataStream<Transaction> personDatabaseTransactions = env
                    .addSource(flinkPravegaReaderTransactions)
                    .uid("transaction")
                    .name("transaction");
            personDatabaseTransactions.printToErr().uid("personDatabaseTransactions-print").name("personDatabaseTransactions-print");

            // Assign timestamps and watermarks based on timestamp in each event.
            final DataStream<Transaction> transactionsWithTimestamps = personDatabaseTransactions
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<Transaction>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(Transaction element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarksSensor")
                    .name("assignTimestampsAndWatermarksSensor");
            transactionsWithTimestamps.printToErr().uid("transactionsWithTimestamps-print").name("transactionsWithTimestamps-print");

            // Calculate embeddings for image data in transactions
            final DataStream<EmbeddingsTransaction> transactionEmbeddings = transactionsWithTimestamps
                    .map(new FaceRecognizerMapFunctionTransaction())
                    .uid("TransactionEmbeddings")
                    .name("transactionEmbeddings");

            // For each camera and window, get the most recent frame.
            final DataStream<VideoFrame> lastVideoFramePerCamera = videoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");

            // This is calculating the embeddings for the located faces in the videoframe.
            final DataStream<VideoFrame> videoFrameEmbeddings = lastVideoFramePerCamera
                    .map(new FaceRecognizerMapFunctionVideoFrame())
                    .uid("videoFrameEmbeddings")
                    .name("videoFrameEmbeddings");
            videoFrameEmbeddings.printToErr().uid("videoFrameEmbeddings-print").name("videoFrameEmbeddings-print");

            final KeyedStream<VideoFrame, Integer> videoFramePerCamera = videoFrameEmbeddings
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera);

            // Schema of the embeddings database in state
            // mapping is person-Id to embedding
            // This is how a person-id String maps to an embedding
            MapStateDescriptor<String, Embedding> bcStateDescriptor =
                    new MapStateDescriptor<>("embeddingBroadcastState", String.class, Embedding.class);

            // broadcast the embeddings database to all sub-tasks.
            BroadcastStream<EmbeddingsTransaction> bcedTransactions = transactionEmbeddings.broadcast(bcStateDescriptor);

            // This process tries to match faces on video frame with faces represented in embeddings database, labels
            // the recognized faces, and maintains embeddings database
            DataStream<VideoFrame> facesRecognized = videoFramePerCamera
                    .connect(bcedTransactions)
                    .process(new FaceRecognizerProcessor());

            // Split output video frames into chunks of 8 MiB or less.
            final DataStream<ChunkedVideoFrame> outChunkedVideoFrames = facesRecognized
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Write chunks to Pravega encoded as JSON.
            final FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            outChunkedVideoFrames
                    .addSink(flinkPravegaWriter)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * A map function that uses TensorFlow and pre-trained facenet model to locate the faces in video frame, and
     * store the embeddings for the faces located.
     * The TensorFlow Session cannot be serialized so it is declared transient and
     * initialized in open().
     */
    static class FaceRecognizerMapFunctionVideoFrame extends RichMapFunction<VideoFrame, VideoFrame> {
        final private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.FaceRecognizerMapFunctionVideoFrame.class);
        private transient FaceRecognizer recognizer;

        /**
         * The first execution takes 6 minutes on a V100.
         * We warmup in open() so that map() does not timeout.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            recognizer = new FaceRecognizer();
            recognizer.warmup();
        }

        @Override
        public void close() {
            recognizer.close();
        }

        @Override
        public VideoFrame map(VideoFrame frame) throws Exception {
            log.info("map: BEGIN: camera={}, frameNumber={}", frame.camera, frame.frameNumber);
            frame.recognizedBoxes = recognizer.locateFaces(frame.data);
            for (BoundingBox faceLocation : frame.recognizedBoxes) {
                Mat imageMat = imdecode(new Mat(frame.data), IMREAD_UNCHANGED);
                byte[] currentFaceData = recognizer.cropFace(faceLocation, imageMat);
                frame.embeddingValues.add(recognizer.embeddFace(currentFaceData));
            }
            log.info("map: END: camera={}, frameNumber={}", frame.camera, frame.frameNumber);
            return frame;
        }
    }

    /**
     * A map function that uses TensorFlow and pre-trained facenet model to locate the faces in images within transactions, and
     * store the embeddings for the faces located.
     * The TensorFlow Session cannot be serialized so it is declared transient and
     * initialized in open().
     */
    static class FaceRecognizerMapFunctionTransaction extends RichMapFunction<Transaction, EmbeddingsTransaction> {
        final private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.FaceRecognizerMapFunctionTransaction.class);
        private transient FaceRecognizer recognizer;

        /**
         * The first execution takes 6 minutes on a V100.
         * We warmup in open() so that map() does not timeout.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            recognizer = new FaceRecognizer();
            recognizer.warmup();
        }

        @Override
        public void close() {
            recognizer.close();
        }

        @Override
        public EmbeddingsTransaction map(Transaction tran) throws Exception {
            EmbeddingsTransaction embTran = new EmbeddingsTransaction(tran.personId, tran.imageName, tran.imageData, tran.transactionType, tran.timestamp, new ArrayList<float[]>());
            if (tran.transactionType.equals("add")) {
                log.info("map: BEGIN:tran={}", tran);
                List<BoundingBox> recognizedBoxes = recognizer.locateFaces(tran.imageData);
                for (BoundingBox faceLocation : recognizedBoxes) {
                    Mat imageMat = imdecode(new Mat(embTran.imageData), IMREAD_UNCHANGED);
                    byte[] currentFaceData = recognizer.cropFace(faceLocation, imageMat);
                    embTran.embeddingValues.add(recognizer.embeddFace(currentFaceData));
                    embTran.imageData = null; // data not needed anymore, since embeddings are calculated
                }
            }
            return embTran;
        }
    }
}
