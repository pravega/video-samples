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
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.Embedding;
import io.pravega.example.common.Transaction;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.JsonDeserializationSchema;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// set --parallelism=1

/**
 * This job populates the embeddings database.
 */
public class FlinkFaceRecognizerJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(FlinkFaceRecognizerJob.class);
    private static FaceRecognizer recognizer;

    public FlinkFaceRecognizerJob(VideoAppConfiguration config) {
        super(config);
        recognizer = new FaceRecognizer();
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
                    .uid("assignTimestampsAndWatermarksSensor")
                    .name("assignTimestampsAndWatermarksSensor");
            transactionsWithTimestamps.printToErr().uid("transactionsWithTimestamps-print").name("transactionsWithTimestamps-print");

            // For each camera and window, get the most recent frame.
            final DataStream<VideoFrame> lastVideoFramePerCamera = videoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");

            final KeyedStream<VideoFrame, Integer> videoFramePerCamera = lastVideoFramePerCamera
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera);

            // Schema of the embeddings database in state
            MapStateDescriptor<String, Embedding> bcStateDescriptor =
                    new MapStateDescriptor<>("embeddingBroadcastState", String.class, Embedding.class);

            // Partition the embeddings database within this stream.
            BroadcastStream<Transaction> bcedTransactions = transactionsWithTimestamps.broadcast(bcStateDescriptor);

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
}
