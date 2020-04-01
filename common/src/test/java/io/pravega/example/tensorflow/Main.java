package io.pravega.example.tensorflow;


import org.apache.commons.io.IOUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

//import sun.nio.ch.IOUtil;

public class Main {
    private final static String IMAGE = "before_output.jpg";

    public static void main(String[] args) {
        TFObjectDetector objectDetector = new TFObjectDetector();
        byte[]    data =  objectDetector.detect(readAllBytesOrExit(IMAGE)).getJpegBytes();

        String fIMAGE = "c:/tmp/final_output.jpg";

        BufferedImage bImage = createImageFromBytes(data);
        saveImage(bImage, fIMAGE);
    }

    private static void printToConsole(final List<Recognition> recognitions) {
        for (Recognition recognition : recognitions) {
            System.out.println("TITLE :  "+recognition.getTitle());
            System.out.println("CONFIDENCE :  "+recognition.getConfidence());
        }
    }

    public static byte[] readAllBytesOrExit(final String fileName) {
        try {
            return IOUtils.toByteArray(
                    new FileInputStream(
                            new File("c:/tmp/" + fileName)
                    )
            );
        } catch (IOException | NullPointerException ex) {

            throw new RuntimeException(ex);
        }
    }

    public static String saveImage(final BufferedImage image, final String target) {
        try {
            ImageIO.write(image,"jpg", new File(target));
            return target;
        } catch (IOException ex) {

            throw new RuntimeException(ex);
        }
    }

    private  static BufferedImage createImageFromBytes(final byte[] imageData) {
        ByteArrayInputStream bais = new ByteArrayInputStream(imageData);
        try {
            return ImageIO.read(bais);
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }
}
