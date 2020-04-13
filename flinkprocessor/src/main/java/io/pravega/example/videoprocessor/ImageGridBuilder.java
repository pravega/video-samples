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
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;


/**
 * Combines multiple images into a square grid of images.
 * Images must be the correct size.
 * See example output in /images/grid-sample.png.
 */
public class ImageGridBuilder {
    private final int imageWidth;
    private final int imageHeight;
    private final int numColumns;
    private final int numRows;
    private final int margin;
    private final int statusWidth;
    private final BufferedImage outImage;

    /**
     *
     * @param imageWidth    Width of each input image.
     * @param imageHeight   Height of each input image.
     * @param numColumns    Number of columns.
     * @param numRows       Number of rows.
     */
    public ImageGridBuilder(int imageWidth, int imageHeight, int numColumns, int numRows) {
        this.imageWidth = imageWidth;
        this.imageHeight = imageHeight;
        this.numColumns = numColumns;
        this.numRows = numRows;
        margin = 1;
        statusWidth = 0;
        int outputWidth = (imageWidth + margin) * this.numColumns - margin + statusWidth;
        int outputHeight = (imageHeight + margin) * this.numRows - margin;
        outImage = new BufferedImage(outputWidth, outputHeight, BufferedImage.TYPE_INT_RGB);
    }

    /**
     *
     * @param position 0-based position.
     * @param image    Image file bytes.
     */
    public void addImage(int position, byte[] image) {
        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(image);
            BufferedImage inImage = ImageIO.read(inStream);
            int x = (position % numColumns) * (imageWidth + margin);
            int y = (position / numColumns) * (imageHeight + margin);
            outImage.getRaster().setRect(x, y, inImage.getData());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param images Map from position to image file bytes.
     */
    public void addImages(Map<Integer, byte[]> images) {
        images.forEach(this::addImage);
    }

    /**
     *
     * @param format "png" for PNG output.
     * @return Image file bytes.
     */
    public byte[] getOutputImageBytes(String format) {
        try {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ImageWriter writer = ImageIO.getImageWritersByFormatName(format).next();
            ImageWriteParam writeParam = writer.getDefaultWriteParam();
            if (format.equals("jpg")) {
                writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                writeParam.setCompressionQuality(0.5f);
            }
            writer.setOutput(new MemoryCacheImageOutputStream(outStream));
            writer.write(null, new IIOImage(outImage, null, null), writeParam);
            return outStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
