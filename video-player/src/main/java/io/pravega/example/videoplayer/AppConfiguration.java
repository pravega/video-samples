package io.pravega.example.videoplayer;

import io.pravega.example.common.PravegaAppConfiguration;

public class AppConfiguration extends PravegaAppConfiguration {
    private final int camera;

    public AppConfiguration(String[] args) {
        super(args);
        camera = Integer.parseInt(getEnvVar("CAMERA", "1000"));
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                super.toString() +
                ", camera=" + camera +
                '}';
    }

    public int getCamera() {
        return camera;
    }
}
