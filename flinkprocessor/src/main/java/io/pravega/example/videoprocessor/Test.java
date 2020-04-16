package io.pravega.example.videoprocessor;


import io.pravega.example.common.VideoFrame;
import io.pravega.example.tensorflow.FaceDetector;
import io.pravega.example.tensorflow.FaceRecognizer;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;

public class Test {
    public static void main(String args[]) throws Exception {
        try{
            InputStream origImage = Test.class.getResourceAsStream("/2020-04-06-181009.jpg");       // The model
            InputStream otherImage = Test.class.getResourceAsStream("/TJ_now.jpg");       // The model

            VideoFrame origFrame = new VideoFrame();
            origFrame.data = IOUtils.toByteArray(origImage);
            FaceDetector.getInstance().detectFaces(origFrame);
            FaceRecognizer.getInstance().recognizeFaces(origFrame);
            System.out.println(origFrame);

            VideoFrame otherFrame = new VideoFrame();
            otherFrame.data = IOUtils.toByteArray(otherImage);
            FaceDetector.getInstance().detectFaces(otherFrame);
            FaceRecognizer.getInstance().recognizeFaces(otherFrame);
            System.out.println(otherFrame);

            float[] origFrameEmbeddings = origFrame.embeddings.get(0);
            float[] otherFrameEmbeddings = otherFrame.embeddings.get(0);
//            double[] diffSq = new double[][origFrameEmbeddings.length];
            double sumDiffSq = 0;

            for(int i=0; i< origFrameEmbeddings.length; i++) {
                sumDiffSq += Math.pow(origFrameEmbeddings[i] - otherFrameEmbeddings[i], 2);
            }

            double euclideanDistance = Math.sqrt(sumDiffSq);
            System.out.println("euclidean disrance is " + euclideanDistance);

            if(euclideanDistance < 1) {
                System.out.println("It matched");
            } else {
                System.out.println("It did not match");
            }
        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}