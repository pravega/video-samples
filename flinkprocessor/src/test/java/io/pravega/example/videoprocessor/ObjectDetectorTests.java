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

import io.pravega.example.tensorflow.TFObjectDetector;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;

public class ObjectDetectorTests {
    private static Logger log = LoggerFactory.getLogger(ObjectDetectorTests.class);

    @Test
    @Ignore
    public void Test1() throws Exception {
        byte[] inBytes = Files.readAllBytes((new File("test_truck.jpg")).toPath());
        byte[] outBytes = new TFObjectDetector().detect(inBytes).getJpegBytes();
        Files.write((new File("detected_test_truck.jpg")).toPath(), outBytes);
    }
}