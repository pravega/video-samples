package io.pravega.example.camerarecorder;

import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.BoxPosition;
import io.pravega.example.tensorflow.ImageUtil;
import io.pravega.example.tensorflow.Recognition;
import org.apache.commons.io.IOUtils;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.opencv.opencv_core.*;
import org.bytedeco.opencv.opencv_objdetect.CascadeClassifier;
import org.bytedeco.opencv.opencv_objdetect.CvHaarClassifierCascade;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.bytedeco.opencv.global.opencv_core.*;
import static org.bytedeco.opencv.global.opencv_imgcodecs.*;
import static org.bytedeco.opencv.global.opencv_imgproc.*;
import static org.bytedeco.opencv.global.opencv_objdetect.CASCADE_SCALE_IMAGE;

public class Test {
    public Test() {
        InputStream inputImage = getClass().getResourceAsStream("/TJ_now.jpg");
        InputStream classifier = getClass().getResourceAsStream("/haarcascade_frontalface_alt.xml");
    }

    public static void main(String args[]) throws IOException, URISyntaxException {
        InputStream imageStream = Test.class.getResourceAsStream("/TJ_now.jpg");
        byte[] imageBytes = IOUtils.toByteArray(imageStream);
        Mat imageMat = imdecode(new Mat(imageBytes), IMREAD_UNCHANGED);
        IplImage inputImage = new IplImage(imageMat);

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
        if (Math.round(height * 0.2f) > 0) {
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



        List<BoundingBox> recognizedBoxes = new ArrayList<BoundingBox>();

        for (int i = 0; i < faces.size(); i++) {
//            double boxX = Math.max(faces.get(i).x() - 10, 0);
//            double boxY = Math.max(faces.get(i).y() - 10, 0);
            double boxX = faces.get(i).x() - 20;
            double boxY = faces.get(i).y() - 20;
            double boxWidth = faces.get(i).width() + 40;
            double boxHeight = faces.get(i).height() + 40;
            double boxConfidence = -1.0;
            double[] boxClasses = new double[1];

//            System.out.println("Rect Width: " + faces.get(i).width() + 20 + ", Height:" + faces.get(i).height() + 20);


//            graphics.drawRect((int) boxX, (int) boxY, (int) boxWidth, (int) boxHeight);

            BoundingBox currentBox = new BoundingBox(boxX, boxY, boxWidth, boxHeight, boxConfidence, boxClasses);
            System.out.println("Rect x:" + currentBox.getX() + ", Y: " + currentBox.getY());
            System.out.println("Bounding Box X:" + currentBox.getX() + ", Y: " + currentBox.getY());

            recognizedBoxes.add(currentBox);
        }


        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "jpg", baos);
        byte[] outData;
        outData = baos.toByteArray();

//        Mat croppedimage = new Mat(imageMat, faces.get(0));
//        byte[] outData = new byte[(int)(croppedimage.total()*croppedimage.elemSize())];
//        imencode(".jpg", croppedimage, outData);

        for(BoundingBox currentFace: recognizedBoxes) {

            Recognition recognition = new Recognition(1, "Thejas", (float)1,
                    new BoxPosition((float) (currentFace.getX()),
                            (float) (currentFace.getY()),
                            (float) currentFace.getWidth(),
                            (float) currentFace.getHeight()));
//            frame.recognitions.add(recognition);

            System.out.println("Recognition location X: " + recognition.getLocation().getLeft() + ", Height:" + recognition.getLocation().getTop());

            outData= ImageUtil.getInstance().labelFace(outData, recognition);
        }

        Mat outputImage = imdecode(new Mat(outData), IMREAD_UNCHANGED);

//        File outputfile = new File("./camera-recorder/src/main/resources/detected_face.jpg");
//        ImageIO.write(bufferedImage, "jpg", outputfile);


        imwrite("./camera-recorder/src/main/resources/detected_face.jpg", outputImage);
    }
}

