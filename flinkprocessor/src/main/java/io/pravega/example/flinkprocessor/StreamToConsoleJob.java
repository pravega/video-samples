package io.pravega.example.flinkprocessor;

import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Display the contents of a Pravega stream as UTF8 strings.
 */
public class StreamToConsoleJob extends AbstractJob {
    private static Logger log = LoggerFactory.getLogger(StreamToConsoleJob.class);

    public StreamToConsoleJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = StreamToConsoleJob.class.getName();
            StreamExecutionEnvironment env = initializeFlinkStreaming();
            createStream(appConfiguration.getInputStreamConfig());
            FlinkPravegaReader<String> flinkPravegaReader = FlinkPravegaReader.<String>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(appConfiguration.getInputStreamConfig().stream)
                    .withDeserializationSchema(new UTF8StringDeserializationSchema())
                    .build();
            DataStream<String> ds = env.addSource(flinkPravegaReader);
            ds.printToErr();
            log.info("Executing {} job", jobName);
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
