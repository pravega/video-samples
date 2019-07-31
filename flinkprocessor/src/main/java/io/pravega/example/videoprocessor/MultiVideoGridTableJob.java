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
import io.pravega.connectors.flink.FlinkPravegaJsonTableSink;
import io.pravega.connectors.flink.FlinkPravegaJsonTableSource;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.flinkprocessor.AppConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Deprecated. Use MultiVideoGridJob instead.
 */
public class MultiVideoGridTableJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(MultiVideoGridTableJob.class);

    public MultiVideoGridTableJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = MultiVideoGridTableJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            createStream(appConfiguration.getInputStreamConfig());
            createStream(appConfiguration.getOutputStreamConfig());
            // Start at the current tail.
            StreamCut startStreamCut = getStreamInfo(appConfiguration.getInputStreamConfig().stream).getTailStreamCut();
//            StreamCut startStreamCut = StreamCut.UNBOUNDED;
            TableSchema inputSchema = TableSchema.builder()
                    .field("timestamp", Types.SQL_TIMESTAMP())
                    .field("frameNumber", Types.INT())
                    .field("camera", Types.INT())
                    .field("ssrc", Types.INT())
                    .field("data", Types.PRIMITIVE_ARRAY(Types.BYTE()))     // PNG file bytes
                    .build();
            FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                    .forStream(appConfiguration.getInputStreamConfig().stream, startStreamCut, StreamCut.UNBOUNDED)
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .failOnMissingField(true)
                    .withRowtimeAttribute("timestamp", new ExistingField("timestamp"), new BoundedOutOfOrderTimestamps(100L))
                    .withSchema(inputSchema)
                    .build();
            tableEnv.registerTableSource("video", source);
            Table t1 = tableEnv.scan("video");
            t1.printSchema();
//            tableEnv.toAppendStream(t1.select("camera, ssrc, frameNumber, timestamp"), Row.class).printToErr();

            // Resize all input images.
            int imageWidth = 100;
            int imageHeight = 100;
            tableEnv.registerFunction("ImageResizer", new ImageResizerUDF(imageWidth, imageHeight));
            Table t2 = t1.select("camera, ssrc, frameNumber, timestamp, ImageResizer(data) as data");
            t2.printSchema();
            tableEnv.toAppendStream(t2.select("camera, ssrc, frameNumber, timestamp"), Row.class).printToErr();

            // Aggregate resized images.
            // For each 100 millisecond window, we take the last image from each camera.
            // Then these images are combined in a square grid.
            // TODO: The window is never triggered if parallelism > 1. See https://emcnautilus.slack.com/archives/C0LJMGMNH/p1559586022017100.
            tableEnv.registerFunction("ImageAggregator", new ImageAggregator(imageWidth, imageHeight));
            Table t3 = t2
                    .window(Tumble.over("100.millis").on("timestamp").as("w"))
                    .groupBy("w")
                    .select("0 as camera, 0 as ssrc, 0 as frameNumber, w.end as timestamp, '' as routing_key" +
                            ", ImageAggregator(camera, data) as data");
            t3.printSchema();
            tableEnv.toAppendStream(t3.select("timestamp"), Row.class).printToErr();

            // Write output to new Pravega stream
            FlinkPravegaJsonTableSink sink = FlinkPravegaJsonTableSink.builder()
                    .forStream(appConfiguration.getOutputStreamConfig().stream)
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .withRoutingKeyField("routing_key")
                    .build();
            t3.writeToSink(sink);

            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ImageResizerUDF extends ScalarFunction {
        private final int imageWidth;
        private final int imageHeight;

        public ImageResizerUDF(int imageWidth, int imageHeight) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
        }

        public byte[] eval(byte[] image) {
            ImageResizer resizer = new ImageResizer(imageWidth, imageHeight);
            return resizer.resize(image);
        }
    }

    public static class ImageAggregatorAccum {
        // Map from camera to last image data.
        public Map<Integer, byte[]> images = new HashMap<>();
    }

    public static class ImageAggregator extends AggregateFunction<byte[], ImageAggregatorAccum> {

        private final int imageWidth;
        private final int imageHeight;

        public ImageAggregator(int imageWidth, int imageHeight) {
            this.imageWidth = imageWidth;
            this.imageHeight = imageHeight;
        }

        @Override
        public ImageAggregatorAccum createAccumulator() {
            return new ImageAggregatorAccum();
        }

        @Override
        public byte[] getValue(ImageAggregatorAccum accum) {
            ImageGridBuilder builder = new ImageGridBuilder(imageWidth, imageHeight, accum.images.size());
            builder.addImages(accum.images);
            return builder.getOutputImageBytes("png");
        }

        public void accumulate(ImageAggregatorAccum accum, int camera, byte[] data) {
            accum.images.put(camera, data);
        }
    }
}
