package io.pravega.example.videoplayer;

import io.pravega.client.stream.Position;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.StreamCutImpl;

/**
 * This incrementally builds a StreamCut by starting with the reader group's
 * starting StreamCut and updating it after each event has been read by
 * this reader.
 * The StreamCut will point to the event immediately following the event
 * whose Position was provided.
 */
public class StreamCutBuilder {
    private final Stream stream;
    private StreamCut streamCut;

    public StreamCutBuilder(Stream stream, StreamCut startStreamCut) {
        this.stream = stream;
        this.streamCut = startStreamCut;
    }

    public void addEvent(Position position) {
        final PositionImpl pos = (PositionImpl) position;
        // TODO: This may not provide a complete stream cut if there are multiple readers in the reader group.
        streamCut = new StreamCutImpl(stream, pos.getOwnedSegmentsWithOffsets());
    }

    public StreamCut getStreamCut() {
        return streamCut;
    }
}
