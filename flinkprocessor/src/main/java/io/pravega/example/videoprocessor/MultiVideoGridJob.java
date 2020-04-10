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
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.VideoFrame;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.lang.Math.max;

/**
 * A Flink job that reads images from multiple cameras stored in a Pravega stream, combines them
 * into a square grid of images (like a security camera monitor), and writes the resulting
 * images to another Pravega stream.
 * Images are chunked into 512 KB chunks to allow for very large images.
 */
public class MultiVideoGridJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) {
        VideoAppConfiguration config = new VideoAppConfiguration(args);
        log.info("config: {}", config);
        MultiVideoGridJob job = new MultiVideoGridJob(config);
        job.run();
    }

    public MultiVideoGridJob(VideoAppConfiguration config) {
        super(config);
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(getConfig().getInputStreamConfig());
            createStream(getConfig().getOutputStreamConfig());

            StreamCut startStreamCut = StreamCut.UNBOUNDED;
            if (getConfig().isStartAtTail()) {
                startStreamCut = getStreamInfo(getConfig().getInputStreamConfig().getStream()).getTailStreamCut();
            }

            // Read chunked video frames from Pravega.
            FlinkPravegaReader<ChunkedVideoFrame> flinkPravegaReader = FlinkPravegaReader.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getInputStreamConfig().getStream(), startStreamCut, StreamCut.UNBOUNDED)
                    .withDeserializationSchema(new ChunkedVideoFrameDeserializationSchema())
                    .build();
            DataStream<ChunkedVideoFrame> inChunkedVideoFrames = env
                    .addSource(flinkPravegaReader)
                    .setParallelism(getConfig().getReaderParallelism())
                    .uid("input-source")
                    .name("input-source");
            inChunkedVideoFrames.printToErr();

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
//            inChunkedVideoFramesWithTimestamps.printToErr().uid("inChunkedVideoFramesWithTimestamps-print").name("inChunkedVideoFramesWithTimestamps-print");

            // Unchunk disabled.
            final DataStream<VideoFrame> inVideoFrames = inChunkedVideoFramesWithTimestamps
                    .map(VideoFrame::new)
                    .uid("ChunkedVideoFrameReassembler")
                    .name("ChunkedVideoFrameReassembler");

            final DataStream<VideoFrame> rebalancedVideoFrames;
            if (getConfig().isEnableRebalance()) {
                rebalancedVideoFrames = inVideoFrames.rebalance();
            } else {
                rebalancedVideoFrames = inVideoFrames;
            }

            // Resize all input images. This will be performed in parallel.
            int imageWidth = getConfig().getImageWidth();
            int imageHeight = getConfig().getImageHeight();
            DataStream<VideoFrame> resizedVideoFrames = rebalancedVideoFrames
                    .map(frame -> {
                        ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
                        frame.data = resizer.resize(frame.data);
                        frame.hash = null;
                        return frame;
                    })
                    .uid("ImageResizer")
                    .name("ImageResizer");
//            resizedVideoFrames.printToErr().uid("resizedVideoFrames-print").name("resizedVideoFrames-print");;

            // Aggregate resized images.
            // For each time window, we take the last image from each camera.
            // Then these images are combined in a square grid.
            // To maintain ordering in the output images, we use parallelism of 1 for all subsequent operations.
            long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            int camera = 0;
            int ssrc = new Random().nextInt();
            DataStream<ImageAggregatorResult> aggResults = resizedVideoFrames
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .aggregate(new ImageAggregator(imageWidth, imageHeight, camera, ssrc))
                    .setParallelism(1)
                    .uid("ImageAggregator")
                    .name("ImageAggregator");

            DataStream<OrderedImageAggregatorResult> ordered = aggResults.process(new ProcessFunction<ImageAggregatorResult, OrderedImageAggregatorResult>() {
                long index = 0;

                @Override
                public void processElement(ImageAggregatorResult value, Context ctx, Collector<OrderedImageAggregatorResult> out) throws Exception {
                    out.collect(new OrderedImageAggregatorResult(index++, value));
                }
            }).setParallelism(1);
//            ordered.printToErr();

            final OutputTag<OrderedImageAggregatorResult> outputTag0 = new OutputTag<>("0", TypeInformation.of(OrderedImageAggregatorResult.class));
            final OutputTag<OrderedImageAggregatorResult> outputTag1 = new OutputTag<>("1", TypeInformation.of(OrderedImageAggregatorResult.class));

            SingleOutputStreamOperator<Integer> split = ordered.process(new ProcessFunction<OrderedImageAggregatorResult, Integer>() {
                @Override
                public void processElement(OrderedImageAggregatorResult value, Context ctx, Collector<Integer> out) throws Exception {
                    if (value.index % 2 == 0) {
                        ctx.output(outputTag0, value);
                    } else {
                        ctx.output(outputTag1, value);
                    }
                }
            });
//            split.printToErr();
            DataStream<OrderedImageAggregatorResult> split0 = split.getSideOutput(outputTag0);
            DataStream<OrderedImageAggregatorResult> split1 = split.getSideOutput(outputTag1);

            DataStream<OrderedVideoFrame> outVideoFrames0 = split0.map(aggResult ->
                    new OrderedVideoFrame(aggResult.index, aggResult.value.asVideoFrame(imageWidth, imageHeight, camera, ssrc, (int) aggResult.index)));
            DataStream<OrderedVideoFrame> outVideoFrames1 = split1.map(aggResult ->
                    new OrderedVideoFrame(aggResult.index, aggResult.value.asVideoFrame(imageWidth, imageHeight, camera, ssrc, (int) aggResult.index)));

            KeyedStream<OrderedVideoFrame, Integer> keyed0 = outVideoFrames0.keyBy((KeySelector<OrderedVideoFrame, Integer>) value -> value.value.camera);
            KeyedStream<OrderedVideoFrame, Integer> keyed1 = outVideoFrames1.keyBy((KeySelector<OrderedVideoFrame, Integer>) value -> value.value.camera);
//            KeyedStream<OrderedVideoFrame, Tuple1> keyed1 = outVideoFrames1.keyBy((KeySelector<OrderedVideoFrame, Tuple1>) value -> Tuple1.of(value.value.camera));
//            KeyedStream<OrderedVideoFrame, Tuple1> keyed0 = outVideoFrames0.keyBy(outVideoFrames0);
            keyed0.printToErr();
            keyed1.printToErr();

            ConnectedStreams<OrderedVideoFrame, OrderedVideoFrame> connected = keyed0.connect(keyed1);

            DataStream<OrderedVideoFrame> combined = connected.process(new OrderedVideoFrameCoProcessFunction());
//            combined.printToErr();

            DataStream<VideoFrame> outVideoFrames = combined.map(x -> x.value);

            outVideoFrames.printToErr().setParallelism(1).uid("outVideoFrames-print").name("outVideoFrames-print");

            // Split output video frames into chunks of 8 MiB or less.
            DataStream<ChunkedVideoFrame> outChunkedVideoFrames = outVideoFrames
                    .flatMap(new VideoFrameChunker(getConfig().getChunkSizeBytes()))
//                    .setParallelism(1)
                    .uid("VideoFrameChunker")
                    .name("VideoFrameChunker");
//            outChunkedVideoFrames.printToErr().setParallelism(1).uid("outChunkedVideoFrames-print").name("outChunkedVideoFrames-print");

            // Write chunks to Pravega encoded as JSON.
            FlinkPravegaWriter<ChunkedVideoFrame> flinkPravegaWriter = FlinkPravegaWriter.<ChunkedVideoFrame>builder()
                    .withPravegaConfig(getConfig().getPravegaConfig())
                    .forStream(getConfig().getOutputStreamConfig().getStream())
                    .withSerializationSchema(new ChunkedVideoFrameSerializationSchema())
                    .withEventRouter(frame -> String.format("%d", frame.camera))
                    .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                    .build();
            outChunkedVideoFrames
                    .addSink(flinkPravegaWriter)
//                    .setParallelism(1)
                    .uid("output-sink")
                    .name("output-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
        // Maximum timestamp from cameras.
        public Timestamp timestamp = new Timestamp(0);

        public ImageAggregatorAccum() {
        }

        public ImageAggregatorAccum(ImageAggregatorAccum accum) {
            this.images = accum.images;
            this.timestamp = accum.timestamp;
        }

        @Override
        public String toString() {
            return "ImageAggregatorAccum{" +
                    "timestamp=" + timestamp +
                    '}';
        }
    }

    public static class ImageAggregatorResult extends ImageAggregatorAccum {
        public ImageAggregatorResult() {
        }

        public ImageAggregatorResult(ImageAggregatorAccum accum) {
            super(accum);
        }

        public VideoFrame asVideoFrame(int imageWidth, int imageHeight, int camera, int ssrc, int frameNumber) {
            final long t0 = System.currentTimeMillis();
            VideoFrame videoFrame = new VideoFrame();
            videoFrame.camera = camera;
            videoFrame.ssrc = ssrc;
            videoFrame.timestamp = timestamp;
            videoFrame.frameNumber = frameNumber;
//            videoFrame.data = new byte[]{};
            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, images.size());
            builder.addImages(images);
            videoFrame.data = builder.getOutputImageBytes("jpg");
            videoFrame.hash = videoFrame.calculateHash();
            videoFrame.tags = new HashMap<>();
            videoFrame.tags.put("numCameras", Integer.toString(images.size()));
            try {
                Thread.sleep(new Random().nextInt(1000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info("asVideoFrame: TIME={}", System.currentTimeMillis() - t0);
            log.trace("asVideoFrame: videoFrame={}", videoFrame);
            return videoFrame;
        }
    }

    public static class OrderedImageAggregatorResult {
        public long index;
        public ImageAggregatorResult value;

        public OrderedImageAggregatorResult(long index, ImageAggregatorResult value) {
            this.index = index;
            this.value = value;
        }

        @Override
        public String toString() {
            return "OrderedImageAggregatorResult{" +
                    "index=" + index +
                    ", value=" + value +
                    '}';
        }
    }

    public static class ImageAggregator implements AggregateFunction<VideoFrame, ImageAggregatorAccum, ImageAggregatorResult> {
        private static Logger log = LoggerFactory.getLogger(ImageAggregator.class);

        private final int imageWidth;
        private final int imageHeight;
        private final int camera;
        private final int ssrc;
        // frameNumber is part of the state. There is only a single partition so this can be an ordinary instance variable.
        // TODO: Store frameNumber in Flink state to maintain value across restarts.
        private int frameNumber;

        public ImageAggregator(int imageWidth, int imageHeight, int camera, int ssrc) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
            this.camera = camera;
            this.ssrc = ssrc;
        }

        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public ImageAggregatorResult getResult(ImageAggregatorAccum accum) {
            return new ImageAggregatorResult(accum);
        }

//        @Override
//        public VideoFrame getResult(ImageAggregatorAccum accum) {
//            final long t0 = System.currentTimeMillis();
//            VideoFrame videoFrame = new VideoFrame();
//            videoFrame.camera = camera;
//            videoFrame.ssrc = ssrc;
//            videoFrame.timestamp = accum.timestamp;
//            videoFrame.frameNumber = frameNumber;
//            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, accum.images.size());
//            builder.addImages(accum.images);
//            videoFrame.data = builder.getOutputImageBytes("jpg");
////            videoFrame.hash = videoFrame.calculateHash();
//            videoFrame.tags = new HashMap<String,String>();
//            videoFrame.tags.put("numCameras", Integer.toString(accum.images.size()));
//            log.info("getResult: TIME={}", System.currentTimeMillis() - t0);
//            log.trace("getResult: videoFrame={}", videoFrame);
//            frameNumber++;
//            return videoFrame;
//        }

        @Override
        public ImageAggregatorAccum add(VideoFrame value, ImageAggregatorAccum accum) {
            log.trace("add: value={}", value);
            accum.images.put(value.camera, value.data);
            accum.timestamp = new Timestamp(max(accum.timestamp.getTime(), value.timestamp.getTime()));
            return accum;
        }

        @Override
        public ImageAggregatorAccum merge(ImageAggregatorAccum a, ImageAggregatorAccum b) {
            // TODO: Accumulator can be made more efficient when multiple frames from the same camera can be merged. For now, don't merge.
            log.info("merge: a={}, b={}", a, b);
            return null;
        }

    }

}
