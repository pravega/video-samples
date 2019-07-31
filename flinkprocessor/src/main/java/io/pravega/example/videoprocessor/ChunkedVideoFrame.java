package io.pravega.example.videoprocessor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
/**
 * Allows a VideoFrame to be split into smaller chunks.
 * VideoFrame.data contains the chunk of data.
 */
public class ChunkedVideoFrame extends VideoFrame {
    // 0-based chunk index
    public short chunkIndex;
    // Number of chunks minus 1
    public short finalChunkIndex;

    public ChunkedVideoFrame() {
    }

    public ChunkedVideoFrame(VideoFrame frame) {
        super(frame);
    }

    @Override
    public String toString() {
        return "ChunkedVideoFrame{" +
                super.toString() +
                ", chunkIndex=" + chunkIndex +
                ", finalChunkIndex=" + finalChunkIndex +
                '}';
    }
}
