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
package io.pravega.example.videoprocessor;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static java.awt.image.BufferedImage.TYPE_3BYTE_BGR;
import static java.lang.Math.min;


/**
 * Generate PNG images for testing.
 * Images will show the camera number and frame number.
 */
public class ImageGenerator {
    private final int width;
    private final int height;

    public ImageGenerator(int width, int height) {
        this.width = width;
        this.height = height;
    }

    /**
     * Generate a PNG image for testing.
     *
     * @return Image file bytes
     */
    public byte[] generate(int camera, int frameNumber) {
        try {
            BufferedImage outImage = new BufferedImage(width, height, TYPE_3BYTE_BGR);

            // Image background will be random bytes to prevent compression.
            byte[] imageBuffer = ((DataBufferByte) outImage.getRaster().getDataBuffer()).getData();
            Random rnd = new Random();
            rnd.nextBytes(imageBuffer);
            Graphics2D graphics = outImage.createGraphics();

            // Write camera and frame number on image.
            float fontSize = min(width, height) * 0.13f;
            Font currentFont = graphics.getFont();
            Font newFont = currentFont.deriveFont(fontSize);
            graphics.setFont(newFont);
            int lineHeight = graphics.getFontMetrics().getHeight();
            graphics.drawString("CAMERA", 5, 5 + lineHeight);
            graphics.drawString(String.format("%04d", camera), 5, 5 + 2*lineHeight);
            graphics.drawString("FRAME", 5, 5 + 3*lineHeight);
            graphics.drawString(String.format("%05d", frameNumber), 5, 5 + 4*lineHeight);

            // Write PNG to byte array.
            ImageWriter writer = ImageIO.getImageWritersByFormatName("png").next();
            ImageWriteParam writeParam = writer.getDefaultWriteParam();
//            writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
//            writeParam.setCompressionQuality(1.0f);
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            writer.setOutput(new MemoryCacheImageOutputStream(outStream));
            writer.write(null, new IIOImage(outImage, null, null), writeParam);
            return outStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
