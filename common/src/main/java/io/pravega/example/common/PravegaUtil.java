package io.pravega.example.common;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;

import java.net.URI;

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

    /**
     * Creates a Pravega stream with a given configuration.
     *
     * @param scope the Pravega configuration.
     * @param streamName the stream name (qualified or unqualified).
     * @param controllerURI the stream configuration (scaling policy, retention policy).
     */
    public static boolean  createStream(String  scope, String streamName, URI controllerURI) {
        boolean result = false;
        // Create client config
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        try(StreamManager streamManager = StreamManager.create(clientConfig);)
        {
            if (CommonParams.isPravegaStandalone()) {
                streamManager.createScope(scope);
            }
            result = streamManager.createStream(scope, streamName, StreamConfiguration.builder().build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
