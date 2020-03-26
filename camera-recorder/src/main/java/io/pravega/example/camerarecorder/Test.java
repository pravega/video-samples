package io.pravega.example.camerarecorder;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

import org.apache.commons.io.IOUtils;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_objdetect.CascadeClassifier;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class Test {
    public Test() {
        InputStream inputImage = getClass().getResourceAsStream("/ben_afflek_input_1.jpg");
        InputStream classifier = getClass().getResourceAsStream("/haarcascade_frontalface_alt.xml");
    }

    public static void main (String args[]) throws IOException, URISyntaxException {
        InputStream imageStream = Test.class.getResourceAsStream("/ben_afflek_input_1.jpg");
        byte[] imageBytes = IOUtils.toByteArray(imageStream);
        Mat imageMat = imdecode(new Mat(imageBytes), IMREAD_UNCHANGED);
        CvArr inputImage = new IplImage(imageMat);

        System.out.println(inputImage);

//        CvArr grayImage = new IplImage();
        CvArr grayImage = cvCreateImage(cvGetSize(inputImage), 8, 1); //converting image to grayscale

        cvCvtColor(inputImage, grayImage, COLOR_BGR2GRAY); // Convert image to grayscale
        cvEqualizeHist(grayImage, grayImage);

//        InputStream classifier = getClass().getResourceAsStream("/haarcascade_frontalface_alt.xml");
//        String classifierPath = Paths.get(Test.class.getResource("/haarcascade_frontalface_alt.xml").toURI()).toFile().getPath();
        String classifierPath = "./camera-recorder/src/main/resources/haarcascade_frontalface_alt.xml";
        CascadeClassifier faceCascade = new CascadeClassifier();
        faceCascade.load(classifierPath);

        RectVector faces = new RectVector();
        int absoluteFaceSize = 0;
        int height = grayImage.arrayHeight();
        if (Math.round(height * 0.2f) > 0)
        {
            absoluteFaceSize = Math.round(height * 0.2f);
        }

        faceCascade.detectMultiScale(cvarrToMat(grayImage), faces, 1.1, 2, 0 | CASCADE_SCALE_IMAGE, new Size(absoluteFaceSize, absoluteFaceSize), new Size());

        System.out.println(imageStream);
        System.out.println();

//        BufferedImage bufferedImage = ImageIO.read(imageStream);
        ByteArrayInputStream bais = new ByteArrayInputStream(imageBytes);
        BufferedImage bufferedImage = ImageIO.read(bais);

        Graphics2D graphics = (Graphics2D) bufferedImage.getGraphics();
        graphics.setColor(Color.green);

        for (int i = 0; i < faces.size(); i++) {
            graphics.drawRect(faces.get(i).x(),faces.get(i).y(), faces.get(i).width(), faces.get(i).height());
        }

        File outputfile = new File("./camera-recorder/src/main/resources/detected_face.jpg");
        ImageIO.write(bufferedImage, "jpg", outputfile);
    }
}

