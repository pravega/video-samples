package io.pravega.example.videoprocessor;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;


/**
 * Resizes images to a fixed size.
 */
public class ImageResizer {
    private final int outputWidth;
    private final int outputHeight;

    public ImageResizer(int outputWidth, int outputHeight) {
        this.outputWidth = outputWidth;
        this.outputHeight = outputHeight;
    }

    /**
     * Resizes images to a fixed size.
     *
     * @param image Image file bytes
     * @return Image file bytes
     */
    public byte[] resize(byte[] image) {
        ByteArrayInputStream inStream = new ByteArrayInputStream(image);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        resize(inStream, outStream);
        return outStream.toByteArray();
    }

    public void resize(InputStream inStream, OutputStream outStream) {
        try {
            BufferedImage inImage = ImageIO.read(inStream);
            Image scaledImage = inImage.getScaledInstance(outputWidth, outputHeight, Image.SCALE_SMOOTH);
            BufferedImage outImage = new BufferedImage(outputWidth, outputHeight, inImage.getType());
            Graphics2D g2d = outImage.createGraphics();
            g2d.drawImage(scaledImage, 0, 0, null);
            g2d.dispose();
            ImageIO.write(outImage, "png", outStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
