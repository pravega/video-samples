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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Random;


/**
 * Resizes images to a fixed size.
 */
public class ImageResizer {
    private static Logger log = LoggerFactory.getLogger(ImageResizer.class);

    private final int outputWidth;
    private final int outputHeight;

    private final Random random = new Random();

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
            final long t0 = System.currentTimeMillis();
            BufferedImage inImage = ImageIO.read(inStream);
            Image scaledImage = inImage.getScaledInstance(outputWidth, outputHeight, Image.SCALE_SMOOTH);
            BufferedImage outImage = new BufferedImage(outputWidth, outputHeight, inImage.getType());
            Graphics2D g2d = outImage.createGraphics();
            g2d.drawImage(scaledImage, 0, 0, null);
            g2d.dispose();
            ImageIO.write(outImage, "png", outStream);
            Thread.sleep(random.nextInt(1000));
            log.info("resize: TIME={}", System.currentTimeMillis() - t0);
        } catch (IOException|InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
