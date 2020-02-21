package io.pravega.example.videoplayer;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.StreamCutImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * This builds a StreamCut based on the Position of an event.
 * The StreamCut will point to the event immediately following the event
 * whose Position was provided. The StreamCut will also point to
 * any unread events in other segments.
 *
 * TODO: Confirm that this works for multi-segment streams and through scaling events.
 */
public class StreamCutBuilder {
    private final Stream stream;
    private Map<Segment, Long> ownedSegmentsWithOffsets = new HashMap<>();

    public StreamCutBuilder(Stream stream) {
        this.stream = stream;
    }

    public void addEvent(Position position) {
        final PositionImpl pos = (PositionImpl) position;
        ownedSegmentsWithOffsets = pos.getOwnedSegmentsWithOffsets();
    }

    public StreamCut getStreamCut() {
        return new StreamCutImpl(stream, ownedSegmentsWithOffsets);
    }
}
