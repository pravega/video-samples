/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.tensorflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.awt.image.BufferedImage.TYPE_3BYTE_BGR;

/**
 * Util class for image processing.
 */
public class ImageUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(ImageUtil.class);

    /**
     * Label image with classes and predictions given by the ThensorFLow
     * @param image buffered image to label
     * @param recognitions list of recognized objects
     * @return JPEG image in a byte array
     */
    public byte[] labelImage(final byte[] image, final List<Recognition> recognitions) {
        LOGGER.info("labelImage: BEGIN");
        byte[] bytes;
        BufferedImage bufferedImage = createImageFromBytes(image);
        float scaleX = (float) bufferedImage.getWidth() / (float) 416;
        float scaleY = (float) bufferedImage.getHeight() / (float) 416;
        Graphics2D graphics = (Graphics2D) bufferedImage.getGraphics();
        graphics.setColor(Color.green);

        for (Recognition recognition: recognitions) {
            BoxPosition box = recognition.getScaledLocation(scaleX, scaleY);
            //set font
            Font myFont = new Font("Courier New", 1, 17);
            graphics.setFont(myFont);
            //draw text
            graphics.drawString(recognition.getTitle() + " " + recognition.getConfidence(), box.getLeft(), box.getTop() - 7);
            // draw bounding box
            graphics.drawRect(box.getLeftInt(),box.getTopInt(), box.getWidthInt(), box.getHeightInt());
        }

        graphics.dispose();

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "jpg", baos);
            bytes = baos.toByteArray();
        }
        catch (IOException e)
        {
            throw  new RuntimeException(e);
        }


        return bytes;
    }

    public byte[] labelFace(final byte[] image, final Recognition recognition) {
        byte[] bytes = null;
        BufferedImage bufferedImage = createImageFromBytes(image);
        float scaleX = (float) bufferedImage.getWidth() / (float) 416;
        float scaleY = (float) bufferedImage.getHeight() / (float) 416;
        Graphics2D graphics = (Graphics2D) bufferedImage.getGraphics();
        graphics.setColor(Color.green);

//        for (Recognition recognition: recognitions) {
        BoxPosition box = recognition.getLocation();
        //set font
        Font myFont = new Font("Courier New", 1, 14);
        graphics.setFont(myFont);
        //draw text

        System.out.println(recognition.getTitle());
        graphics.drawString(recognition.getTitle(), box.getLeft(), box.getTop() - 7);
        // draw bounding box
        System.out.println("Recognition location X: " + box.getLeftInt() + ", Height:" + box.getTopInt());
        graphics.drawRect(box.getLeftInt(),box.getTopInt(), box.getWidthInt(), box.getHeightInt());
//        }

        graphics.dispose();

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "jpg", baos);
            bytes = baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("labelImage: END");
        return bytes;
    }

    /**
     * @return JPEG image in a byte array
     */
    public byte[] createBlankJpeg(int width, int height) {
        try {
            BufferedImage outImage = new BufferedImage(width, height, TYPE_3BYTE_BGR);
            ImageWriter writer = ImageIO.getImageWritersByFormatName("jpg").next();
            ImageWriteParam writeParam = writer.getDefaultWriteParam();
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            writer.setOutput(new MemoryCacheImageOutputStream(outStream));
            writer.write(null, new IIOImage(outImage, null, null), writeParam);
            return outStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Saves image into target file name
     * @param image to save
     * @param target file name to save image
     * @return location of the saved image
     */
    public String saveImage(final BufferedImage image, final String target) {
        try {
            ImageIO.write(image,"jpg", new File(target));
            return target;
        } catch (IOException ex) {
            LOGGER.error("Unagle to save image {}!", target);
            throw new RuntimeException(ex);
        }
    }

    private BufferedImage createImageFromBytes(final byte[] imageData) {
        ByteArrayInputStream bais = new ByteArrayInputStream(imageData);
        try {
            return ImageIO.read(bais);
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }
}
