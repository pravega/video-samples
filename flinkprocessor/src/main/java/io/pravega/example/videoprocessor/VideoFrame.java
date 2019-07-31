package io.pravega.example.videoprocessor;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * A class for storing a single video frame.
 */
public class VideoFrame {
    // Unique ID for this video stream.
    public int camera;
    // Random source identifier used to avoid corruption if multiple sources use the same camera frameNumber.
    // See https://tools.ietf.org/html/rfc3550.
    public int ssrc;
    public Timestamp timestamp;
    public int frameNumber;
    // PNG-encoded image.
    public byte[] data;
    // Truncated SHA-1 hash of data. This is used to confirm that chunking and reassembly do not corrupt the data.
    public byte[] hash;

    public VideoFrame() {
    }

    public VideoFrame(VideoFrame frame) {
        this.camera = frame.camera;
        this.ssrc = frame.ssrc;
        this.timestamp = frame.timestamp;
        this.frameNumber = frame.frameNumber;
        this.data = frame.data;
        this.hash = frame.hash;
    }

    @Override
    public String toString() {
        int sizeToPrint = data.length;
        int maxSizeToPrint = 10;
        if (sizeToPrint > maxSizeToPrint) {
            sizeToPrint = maxSizeToPrint;
        }
        byte[] dataBytes = Arrays.copyOf(data, sizeToPrint);
        String dataStr = Arrays.toString(dataBytes);
        return "VideoFrame{" +
                "camera=" + camera +
                ", ssrc=" + ssrc +
                ", timestamp=" + timestamp +
                ", frameNumber=" + frameNumber +
                ", hash=" + Arrays.toString(hash) +
                ", data(" + data.length + ")=" + dataStr +
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

    public void validateHash() {
        byte[] calculatedHash = calculateHash();
        if (!MessageDigest.isEqual(calculatedHash, hash)) {
            throw new RuntimeException(new DigestException());
        }
    }
}
