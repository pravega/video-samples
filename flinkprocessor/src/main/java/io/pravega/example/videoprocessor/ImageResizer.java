/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Resizes images to a fixed size.
 */
public class ImageResizer {
    private static Logger log = LoggerFactory.getLogger(ImageResizer.class);

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

    /**
     * Resize images. The output is a PNG because this can be encoded faster than JPEG and the output is expected
     * to go to a subsequent task before it is stored.
     */
    public void resize(InputStream inStream, OutputStream outStream) {
        try {
            log.info("resize: BEGIN");
            final long t0 = System.currentTimeMillis();
            BufferedImage inImage = ImageIO.read(inStream);
            Image scaledImage = inImage.getScaledInstance(outputWidth, outputHeight, Image.SCALE_SMOOTH);
            BufferedImage outImage = new BufferedImage(outputWidth, outputHeight, inImage.getType());
            Graphics2D g2d = outImage.createGraphics();
            g2d.drawImage(scaledImage, 0, 0, null);
            g2d.dispose();
            ImageIO.write(outImage, "png", outStream);
            log.info("resize: END: TIME={}", System.currentTimeMillis() - t0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
