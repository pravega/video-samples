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

import com.google.zxing.NotFoundException;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.JsonDeserializationSchema;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.flink.api.common.functions.RichMapFunction;
import io.pravega.example.tensorflow.QRCode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bytedeco.opencv.opencv_core.Mat;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.max;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imencode;

import static org.bytedeco.opencv.global.opencv_imgcodecs.IMREAD_UNCHANGED;
import static org.bytedeco.opencv.global.opencv_imgcodecs.imdecode;

// set --parallelism=1

/**
 * This job populates the embeddings database.
 */
public class FlinkFaceRecognizerJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.class);
    private static FaceRecognizer recognizer;
    private static QRCode badgeProcessor;

    public FlinkFaceRecognizerJob(VideoAppConfiguration config) {
        super(config);
        recognizer = new FaceRecognizer();
        badgeProcessor = new QRCode();
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
            createStream(getConfig().getBadgeStreamConfig());

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
            // Create non-video person database badge datastream.
            //

            final StreamCut startStreamCutBadge = resolveStartStreamCut(getConfig().getBadgeStreamConfig());
            final StreamCut endStreamCutBadge = resolveEndStreamCut(getConfig().getBadgeStreamConfig());

            final FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReaderBadge = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCutBadge, endStreamCutBadge)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            final DataStream<ChunkedVideoFrame> chunkedVideoFrameBadges = env
                    .addSource(flinkPravegaReaderBadge)
                    .uid("badges")
                    .name("badges");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            final DataStream<ChunkedVideoFrame> chunkedVideoFrameBadgesWithTimestamps = chunkedVideoFrameBadges
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<ChunkedVideoFrame>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(ChunkedVideoFrame element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarksBadges")
                    .name("assignTimestampsAndWatermarksBadges");

            // Unchunk (disabled).
            // Operator: ChunkedVideoFrameReassembler
            final DataStream<VideoFrame> videoFrameBadges = chunkedVideoFrameBadgesWithTimestamps
                    .map(VideoFrame::new)
                    .uid("ChunkedVideoFrameReassemblerBadges")
                    .name("ChunkedVideoFrameReassemblerBadges");


            //
            // Create non-video person database transaction datastream.
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

            // Assign timestamps and watermarks based on timestamp in each chunk.
            final DataStream<Transaction> transactionsWithTimestamps = personDatabaseTransactions
                    .assignTimestampsAndWatermarks(
                            new BoundedOutOfOrdernessTimestampExtractor<Transaction>(
                                    Time.milliseconds(getConfig().getMaxOutOfOrdernessMs())) {
                                @Override
                                public long extractTimestamp(Transaction element) {
                                    return element.timestamp.getTime();
                                }
                            })
                    .uid("assignTimestampsAndWatermarksTransaction")
                    .name("assignTimestampsAndWatermarksTransaction");
            transactionsWithTimestamps.printToErr().uid("transactionsWithTimestamps-print").name("transactionsWithTimestamps-print");

            final DataStream<VideoFrame> videoFrameEmbeddings = videoFrames
                    .map(new FaceRecognizerMapFunction())
                    .uid("videoFrameEmbeddings")
                    .name("videoFrameEmbeddings");
            videoFrameEmbeddings.printToErr().uid("videoFrameEmbeddings-print").name("videoFrameEmbeddings-print");


            // For each camera and window, get the most recent frame.
            final DataStream<VideoFrame> lastVideoFramePerCamera = videoFrameEmbeddings
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");

            // For each camera and window, get the most recent badges processed within 5 sec.
            final DataStream<Iterable> lastBadges = videoFrameBadges
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                    .process(new ProcessWindowFunction<VideoFrame, Iterable, Integer, TimeWindow>() {
                        @Override
                        public void process(Integer integer, Context context, Iterable<VideoFrame> elements, Collector<Iterable> out) throws Exception {
                            List<Tuple3<String, Timestamp, Integer>> result = new ArrayList<Tuple3<String, Timestamp, Integer>>();

                            elements.forEach(element -> {
                                String qrVal = null;
                                try {
                                    qrVal = badgeProcessor.readQRCode(element.data);
                                    result.add(new Tuple3<>(qrVal, element.timestamp, element.camera));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                } catch (NotFoundException e) {
                                    // skip if not found
                                }

                                log.info("reached");
                                log.info("qr info is: " + qrVal);
                            });

                            out.collect(result);
                        }
                    })
                    .uid("lastBadgesPerCamera")
                    .name("lastBadgesPerCamera");
            lastBadges.printToErr().uid("lastBadges-print").name("lastBadges-print");

            // window: sliding window: 100 ms
            // process window function: convert iterator of qr codes to list of qr codes: <Timestamp, Camera, List<QR code>>
            // 5 sec window
            // alerts: unknown person, recognized but no badge, qr code but doesnt exist in embeddings database

            // broadcast the list of qr codes onto qr stream, and store list of qr codes into videoframe

            final DataStream<VideoFrame>  videoFrameBadges = lastVideoFramePerCamera
                    .map(frame -> {
                        frame.lastBadges = lastBadges;
                            })
                    .uid("videoFrameBadges")
                    .name("videoFrameBadges");


            final KeyedStream<VideoFrame, Integer> videoFramePerCamera = lastVideoFramePerCamera
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera);

            // Schema of the embeddings database in state
            // mapping is person-Id to embedding
            MapStateDescriptor<String, Embedding> bcStateDescriptor =
            MapStateDescriptor<String, Embedding> bcEmbeddingsStateDescriptor =
                    new MapStateDescriptor<>("embeddingBroadcastState", String.class, Embedding.class);


//            // Schema of the last badges processed in state
//            MapStateDescriptor<String, Embedding> bcBadgesStateDescriptor =
//                    new MapStateDescriptor<>("embeddingBroadcastState", String.class, Embedding.class);



            // Partition the embeddings database within this stream.
            BroadcastStream<Transaction> bcedTransactions = transactionsWithTimestamps.broadcast(bcEmbeddingsStateDescriptor);

            // Run facial recognition on incoming video frames
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
 * A map function that uses TensorFlow.
 * The TensorFlow Session cannot be serialized so it is declared transient and
 * initialized in open().
 */
static class FaceRecognizerMapFunction extends RichMapFunction<VideoFrame, VideoFrame> {
    final private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.FaceRecognizerMapFunction.class);
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
        for(BoundingBox faceLocation: frame.recognizedBoxes) {
            Mat imageMat = imdecode(new Mat(frame.data), IMREAD_UNCHANGED);
            byte[] currentFaceData = recognizer.cropFace(faceLocation, imageMat);
            frame.embeddings.add(recognizer.embeddFace(currentFaceData));
        }
        log.info("map: END: camera={}, frameNumber={}", frame.camera, frame.frameNumber);
        return frame;
    }
}
