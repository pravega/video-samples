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

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.awt.image.BufferedImage.TYPE_3BYTE_BGR;
import static java.lang.Math.min;

public class ImageProcessingTests {
    private static Logger log = LoggerFactory.getLogger(ImageProcessingTests.class);

    @Test
    @Ignore
    public void Test1() throws Exception {

        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        ByteArrayInputStream inStream = new ByteArrayInputStream(inBytes);
        BufferedImage inImage = ImageIO.read(inStream);
        log.info("inImage={}", inImage);

        BufferedImage outImage = new BufferedImage(inImage.getWidth()*2, inImage.getHeight()*2, inImage.getType());
        outImage.getRaster().setRect(0, 0, inImage.getData());
        log.info("outImage={}", outImage);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ImageIO.write(outImage, "png", outStream);

//        ImageWriter writer = ImageIO.getImageWritersByFormatName("png").next();
//        ImageWriteParam writeParam = writer.getDefaultWriteParam();
//        writeParam.setCompressionMode(ImageWriteParam.MODE_DISABLED); // Not supported exception
//        writer.setOutput(outStream);
//        writer.write(null, new IIOImage(outImage, null, null), writeParam);

        byte[] outBytes = outStream.toByteArray();
        Files.write((new File("/tmp/out2.png")).toPath(), outBytes);
    }

    @Test
    @Ignore
    public void Test2() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        Map<Integer, byte[]> images = new HashMap<>();
        images.put(0, inBytes);
        images.put(3, inBytes);
        ImageGridBuilder builder = new ImageGridBuilder(19, 19, 4);
        builder.addImage(1, inBytes);
        builder.addImages(images);
        byte[] outBytes = builder.getOutputImageBytes("png");
        Files.write((new File("/tmp/out3.png")).toPath(), outBytes);
    }

    @Test
    @Ignore
    public void Test3() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("/tmp/camera0-0.png")).toPath());
        ImageResizer resizer = new ImageResizer(100, 100);
        byte[] outBytes = resizer.resize(inBytes);
        Files.write((new File("/tmp/out4.png")).toPath(), outBytes);
    }

    @Test
    @Ignore
    public void Test4() throws Exception {
        Random rnd = new Random();
        int camera = 1;
        int frameNumber = 120;
        int width = 1000;
        int height = width;
        BufferedImage outImage = new BufferedImage(width, height, TYPE_3BYTE_BGR);

        // Image background will be random bytes to prevent compression.
        byte[] imageBuffer = ((DataBufferByte) outImage.getRaster().getDataBuffer()).getData();
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

        ImageWriter writer = ImageIO.getImageWritersByFormatName("png").next();
        ImageWriteParam writeParam = writer.getDefaultWriteParam();
//        writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
//        writeParam.setCompressionQuality(1.0f);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        writer.setOutput(new MemoryCacheImageOutputStream(outStream));
        writer.write(null, new IIOImage(outImage, null, null), writeParam);
        Files.write((new File("/tmp/test4.png")).toPath(), outStream.toByteArray());
    }

    @Test
    @Ignore
    public void Test5() throws Exception {
        ImageGenerator generator = new ImageGenerator(2000, 1000);
        byte[] outBytes = generator.generate(20, 1000);
        Files.write((new File("/tmp/test5.png")).toPath(), outBytes);
    }
}
