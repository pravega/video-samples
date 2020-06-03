package io.pravega.example.videoprocessor;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.example.common.ChunkedVideoFrame;
import io.pravega.example.common.VideoFrame;
import io.pravega.example.flinkprocessor.AbstractJob;
import io.pravega.example.tensorflow.QRCode;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class TwoReaderSameStream extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(TwoReaderSameStream.class);

    public TwoReaderSameStream(VideoAppConfiguration config) {
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
        TwoReaderSameStream job = new TwoReaderSameStream(config);
        job.run();
    }

    @Override
    public VideoAppConfiguration getConfig() {
        return (VideoAppConfiguration) super.getConfig();
    }

    public void run() {
        try {
            final long periodMs = (long) (1000.0 / getConfig().getFramesPerSec());
            final String jobName = TwoReaderSameStream.class.getName();
            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            createStream(getConfig().getInputStreamConfig());
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
                    .forStream(getConfig().getBadgeStreamConfig().getStream(), startStreamCutBadge, endStreamCutBadge)
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

            // For each camera and window, get the most recent frame.
            final DataStream<VideoFrame> lastVideoFramePerCamera = videoFrames
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(periodMs)))
                    .maxBy("timestamp")
                    .uid("lastVideoFramePerCamera")
                    .name("lastVideoFramePerCamera");
            lastVideoFramePerCamera.printToErr().uid("lastVideoFramePerCamera-print").name("lastVideoFramePerCamera-print");

            // For each camera and window, get the most recent badges processed within 5 sec.
            final DataStream<Set> lastBadges = videoFrameBadges
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera)
                    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                    .process(new ProcessWindowFunction<VideoFrame, Set, Integer, TimeWindow>() {
                        @Override
                        public void process(Integer integer, Context context, Iterable<VideoFrame> elements, Collector<Set> out) throws Exception {
                            out.collect(new HashSet());
                        }
                    })
                    .uid("lastBadgesPerCamera")
                    .name("lastBadgesPerCamera");
            lastBadges.printToErr().uid("lastBadges-print").name("lastBadges-print");

            // broadcast the list of qr codes onto qr stream, and store list of qr codes into videoframe

            final KeyedStream<VideoFrame, Integer> videoFramePerCamera = lastVideoFramePerCamera
                    .keyBy((KeySelector<VideoFrame, Integer>) value -> value.camera);

            MapStateDescriptor<Void, Set> bcBadgesStateDescriptor =
                    new MapStateDescriptor("badgesBroadcastState", Void.class, Set.class);

            BroadcastStream<Set> bcedBadges = lastBadges.broadcast(bcBadgesStateDescriptor);

            DataStream<VideoFrame> badgesProcessed = videoFramePerCamera
                    .connect(bcedBadges)
                    .process(new BadgesProcessor());

            final DataStream<ChunkedVideoFrame> outChunkedVideoFrames = badgesProcessed
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
