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

import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.Utils;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.tensorflow.TFObjectDetector;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

//import sun.net.www.content.image.png;

/**
 * This job reads and writes a video stream from Pravega and writes frame metadata to the console.
 */
public class FlinkObjectDetectorJob extends AbstractJob {
    // Logger initialization
    private static Logger log = LoggerFactory.getLogger(FlinkObjectDetectorJob.class);
//    final TFObjectDetector objectDetector = new TFObjectDetector();


    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        FlinkObjectDetectorJob job = new FlinkObjectDetectorJob(config);
        job.run();
    }

    public FlinkObjectDetectorJob(VideoAppConfiguration config) throws IOException, InterruptedException {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            long start = System.currentTimeMillis();
            final String jobName = FlinkObjectDetectorJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());

            StreamCut startStreamCut = StreamCut.UNBOUNDED;
            if (getConfig().isStartAtTail()) {
                startStreamCut = getStreamInfo(getConfig().getInputStreamConfig().getStream()).getTailStreamCut();
            }

            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();

            DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .uid("input-source")
                    .name("input-source");

            // Assign timestamps and watermarks based on timestamp in each chunk.
            DataStream<ChunkedVideoFrame> inChunkedVideoFramesWithTimestamps = inChunkedVideoFrames
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
            // inChunkedVideoFramesWithTimestamps.printToErr().uid("inChunkedVideoFramesWithTimestamps-print").name("inChunkedVideoFramesWithTimestamps-print");

            // Reassemble whole video frames from chunks.
            boolean failOnError = false;
            DataStream<VideoFrame> videoFrames = inChunkedVideoFramesWithTimestamps
                    .keyBy("camera")
                    .window(new ChunkedVideoFrameWindowAssigner())
                    .process(new ChunkedVideoFrameReassembler().withFailOnError(failOnError))
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");
            // videoFrames.printToErr().uid("videoFrames-print").name("videoFrames-print");

            // Parse image file and obtain metadata.
           /* DataStream<String> frameInfo = videoFrames
                    .map(frame -> {
                        InputStream inStream = new ByteArrayInputStream(frame.data);
                        BufferedImage inImage = ImageIO.read(inStream);
                        return String.format("camera %d, frame %d, %dx%dx%d, %d bytes, %s",
                                frame.camera,
                                frame.frameNumber,
                                inImage.getWidth(),
                                inImage.getHeight(),
                                inImage.getColorModel().getNumColorComponents(),
                                frame.data.length,
                                inImage.toString());
                    })
                    .uid("frameInfo")
                    .name("frameInfo");*/
            //  frameInfo.printToErr().uid("frameInfo-print").name("frameInfo-print");

            //  identify objects with YOLOv3
            DataStream<VideoFrame> objectDetectedFrames = videoFrames
                    .map(frame -> {
//                        frame.data = frame.data;
                        frame.data = TFObjectDetector.getInstance().detect(frame.data);
//                      frame.recognitions = new ArrayList<Recognition>();
//                      for(Recognition rec : TFObjectDetector.getInstance().getRecognitions()) {
//                          frame.recognitions.add(rec);
//                      }
                        return frame;
                    });
            objectDetectedFrames.printToErr().uid("video-object-detector-print").name("video-object-detector-print");

            //change to use from input
            Utils.createStream(getConfig().getPravegaConfig(),getConfig().getOutputStreamConfig().getStream().getStreamName());


            DataStream<ChunkedVideoFrame> chunkedVideoFrames = objectDetectedFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");

            // Print to screen a small subset.
            chunkedVideoFrames
                    .filter(f -> f.camera == 0 && f.frameNumber % 10 == 0)
                    .printToErr().uid("chunkedVideoFrames-print").name("chunkedVideoFrames-print");

            System.out.println("Reached chunked");

            // create the Pravega sink to write a stream of video frames
            FlinkPravegaWriter<ChunkedVideoFrame> writer = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            chunkedVideoFrames
                    .addSink(writer)
                    .uid("output-sink")
                    .name("output-sink");


            System.out.println("output-stream: " + getConfig().getOutputStreamConfig().toString());

            chunkedVideoFrames.addSink(writer).name(getConfig().getOutputStreamConfig().toString());
//            videoFrames.addSink(writer).name("video-objects-detected");

            long end = System.currentTimeMillis();
            System.out.println("@@@@@@@@@@@  TIME TAKEN FOR FLINK PROCESS @@@@@@@@@@@  "+(end - start));

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<VideoFrame> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(VideoFrame event) {
            return "SomeRoutingKey";
        }
    }

}