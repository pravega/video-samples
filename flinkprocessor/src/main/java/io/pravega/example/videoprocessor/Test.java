package io.pravega.example.videoprocessor;


import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.FaceDetector;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;

public class Test {
    public static void main(String args[]) throws Exception {
        try{
            InputStream otherImage = Test.class.getResourceAsStream("/TJ_now.jpg");       // The model

            VideoFrame otherFrame = new VideoFrame();
            otherFrame.data = IOUtils.toByteArray(otherImage);
            FaceRecognizer.getInstance().detectFaces(otherFrame.data);
            FaceRecognizer.getInstance().recognizeFaces(otherFrame);
            System.out.println(otherFrame);

            System.out.println(otherFrame.recognitions.get(0).getTitle());

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}