package io.pravega.example.video;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;

public class PravegaUtil {
    /**
     * If the Pravega stream does not exist, creates a new stream with the specified stream configuration.
     * If the stream exists, it is unchanged.
     */
    static public void createStream(ClientConfig clientConfig, PravegaAppConfiguration.StreamConfig streamConfig) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(streamConfig.getScalingPolicy())
                    .build();
            streamManager.createStream(
                    streamConfig.getStream().getScope(),
                    streamConfig.getStream().getStreamName(),
                    streamConfiguration);
        }
    }
}
