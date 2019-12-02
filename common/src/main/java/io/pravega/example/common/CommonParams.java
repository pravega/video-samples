package io.pravega.example.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.net.URI;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Mesos, Kubernetes, etc.
public class CommonParams {
    // By default, we will connect to a standalone Pravega running on localhost.
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER_URI", "tcp://localhost:9090"));
    }

    public static boolean  isPravegaStandalone() {
        return Boolean.parseBoolean(getParam(Constants.PRAVEGA_STANDALONE));
    }

    public static String getUser() {
        return getEnvVar("PRAVEGA_STANDALONE_USER", "admin");
    }

    public static String getPassword() {
        return getEnvVar("PRAVEGA_STANDALONE_PASSWORD", "1111_aaaa");
    }

    public static String getScope() {
        return getEnvVar("PRAVEGA_SCOPE", "video-demo");
    }

    public static String getStreamName() {
        return getEnvVar("STREAM_NAME", "video-demo-stream");
    }

    public static String getRoutingKeyAttributeName() {
        return getEnvVar("ROUTING_KEY_ATTRIBUTE_NAME", "video-demo-key");
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }


    public static ParameterTool params = null;
    public static void init(String[] args)
    {
        params = ParameterTool.fromArgs(args);
    }

    public static String getParam(String key)
    {
        if(params != null && params.has(key))
        {
            return params.get(key);
        }
        else
        {
            return  getDefaultParam(key);
        }
    }

    private static String  getDefaultParam(String key)
    {
        String keyValue = null;
        if(key != null)
        {
            switch (key) {
                case "pravega_scope":
                    keyValue = "video-demo";
                    break;
                case "stream_name":
                    keyValue = "video-demo-stream";
                    break;
                case "pravega_controller_uri":
                    keyValue = "tcp://localhost:9090";
                    break;
                case "routing_key_attribute_name":
                    keyValue = "video-demo-key";
                    break;
                case "pravega_standalone":
                    keyValue = "true";
                    break;
                default:
                    keyValue = null;
            }
        }
        return keyValue;
    }
}
