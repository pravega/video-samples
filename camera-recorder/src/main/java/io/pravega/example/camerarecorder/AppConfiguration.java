package io.pravega.example.camerarecorder;

import io.pravega.example.video.PravegaAppConfiguration;

public class AppConfiguration extends PravegaAppConfiguration {
    private final int imageWidth;
    private final int imageHeight;
    private final double framesPerSec;
    private final int cameraDeviceNumber;
    private final int camera;

    public AppConfiguration(String[] args) {
        super(args);
        imageWidth = Integer.parseInt(getEnvVar("IMAGE_WIDTH", "320"));
        imageHeight = Integer.parseInt(getEnvVar("IMAGE_HEIGHT", "200"));
        framesPerSec = Double.parseDouble(getEnvVar("FRAMES_PER_SEC", "0.25"));
        cameraDeviceNumber = Integer.parseInt(getEnvVar("CAMERA_DEVICE_NUMBER", "0"));
        camera = Integer.parseInt(getEnvVar("CAMERA", "8"));
    }

    public int getImageWidth() {
        return imageWidth;
    }

    public int getImageHeight() {
        return imageHeight;
    }

    public double getFramesPerSec() {
        return framesPerSec;
    }

    public int getCameraDeviceNumber() {
        return cameraDeviceNumber;
    }

    public int getCamera() {
        return camera;
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                super.toString() +
                ", imageWidth=" + imageWidth +
                ", imageHeight=" + imageHeight +
                ", framesPerSec=" + framesPerSec +
                ", cameraDeviceNumber=" + cameraDeviceNumber +
                ", camera=" + camera +
                '}';
    }
}
