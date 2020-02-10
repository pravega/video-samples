package io.pravega.example.htmlvideoplayer;

import io.pravega.example.common.PravegaAppConfiguration;

import java.net.URI;

public class AppConfiguration extends PravegaAppConfiguration {
    private final int camera;
    private final URI serverURI;

    public AppConfiguration(String[] args) {
        super(args);
        camera = Integer.parseInt(getEnvVar("CAMERA", "3"));
        serverURI = URI.create(getEnvVar("SERVER_URI", "http://0.0.0.0:3001/"));
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                super.toString() +
                ", camera=" + camera +
                ", serverURI=" + serverURI +
                '}';
    }

    public int getCamera() {
        return camera;
    }

    public URI getServerURI() {
        return serverURI;
    }
}
