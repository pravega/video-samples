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
package io.pravega.example.common;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import io.pravega.example.tensorflow.BoundingBox;
import io.pravega.example.tensorflow.Recognition;
import org.bytedeco.opencv.opencv_core.RectVector;

import java.io.Serializable;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A class for storing a single video frame.
 */
public class VideoFrame implements Serializable {
    // Unique ID for this video stream.
    public int camera;
    // Random source identifier used to avoid corruption if multiple sources use the same camera and timestamp.
    // See https://tools.ietf.org/html/rfc3550.
    public int ssrc;
    // Event time of this frame. We use Timestamp to have nanosecond precision for high-speed cameras.
    public Timestamp timestamp;
    // Sequential frame number. This can be used to identify any missing frames.
    public int frameNumber;
    // PNG-encoded image.
    public byte[] data;
    // Truncated SHA-1 hash of data. This is used to confirm that chunking and reassembly do not corrupt the data.
    public byte[] hash;
    // Arbitrary user-defined key/value pairs.
    public Map<String,String> tags;
    public KittiSensorReading kittiSensorReadings;
    public List<Recognition> recognitions = new ArrayList<>();
    public ExtendedEventPointer sourceEventPointer;
    public Iterable lastBadges = new ArrayList();

    @JsonIgnore
    public EventReadMetadata eventReadMetadata;

    public List<BoundingBox> recognizedBoxes = new ArrayList<BoundingBox>();

    public List<float[]> embeddings = new ArrayList<float[]>();

    public VideoFrame() {
    }

    public VideoFrame(VideoFrame frame) {
        this.camera = frame.camera;
        this.ssrc = frame.ssrc;
        this.timestamp = frame.timestamp;
        this.frameNumber = frame.frameNumber;
        this.data = frame.data;
        this.hash = frame.hash;
        this.tags = frame.tags;
        this.kittiSensorReadings = frame.kittiSensorReadings;
        this.recognitions = frame.recognitions;
        this.sourceEventPointer = frame.sourceEventPointer;
        this.eventReadMetadata = frame.eventReadMetadata;
        this.recognizedBoxes = frame.recognizedBoxes;
        this.embeddings = frame.embeddings;
        this.lastBadges = frame.lastBadges;
    }

    @Override
    public String toString() {
        String dataStr = "null";
        int dataLength = 0;
        if (data != null) {
            dataLength = data.length;
            int sizeToPrint = dataLength;
            int maxSizeToPrint = 10;
            if (sizeToPrint > maxSizeToPrint) {
                sizeToPrint = maxSizeToPrint;
            }
            dataStr = Arrays.toString(Arrays.copyOf(data, sizeToPrint));
        }
        String tagsStr = "null";
        if (tags != null) {
            tagsStr = tags.keySet().stream()
                    .map(key -> key + "=" + tags.get(key))
                    .collect(Collectors.joining(", ", "{", "}"));
        }
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", tags=" + tagsStr +
                ", hash=" + Arrays.toString(hash) +
                ", kittiSensorReadings=" + kittiSensorReadings +
                ", recognitions=" + recognitions.toString() +
                ", sourceEventPointer=" + sourceEventPointer +
                ", eventReadMetadata=" + eventReadMetadata +
                ", recognizedBoxes=" + recognizedBoxes.toString() +
                ", embeddings=" + embeddings.toString() +
                ", lastBadges=" + StreamSupport
                .stream(lastBadges.spliterator(), false)
                .collect(Collectors.toList()) +
                ", data(" + dataLength + ")=" + dataStr +
                "}";
    }

    public byte[] calculateHash() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return Arrays.copyOf(md.digest(data), 6);
    }

    public void validateHash() throws DigestException {
        if (hash == null) {
            return;
        }
        byte[] calculatedHash = calculateHash();
        if (!MessageDigest.isEqual(calculatedHash, hash)) {
            throw new DigestException();
        }
    }
}
