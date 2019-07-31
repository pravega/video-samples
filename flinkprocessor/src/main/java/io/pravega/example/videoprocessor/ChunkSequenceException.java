package io.pravega.example.videoprocessor;

public class ChunkSequenceException extends RuntimeException {
    public ChunkSequenceException(String s) {
        super(s);
    }
}
