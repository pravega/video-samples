package io.pravega.example.common;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.EventPointerExposed;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamCutInternal;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Identifies an event in a Pravega stream.
 * This can be used for fetching this single event or creating a reader group that starts at this event
 * or ends immediately after this event.
 */
public class ExtendedEventPointer implements Serializable {
    final private byte[] eventPointerBytes;
    final private String startStreamCutText;
    final private String endStreamCutText;
    final private String description;

    public ExtendedEventPointer() {
        eventPointerBytes = new byte[0];
        startStreamCutText = "";
        endStreamCutText = "";
        description = "";
    }

    /**
     * TODO: The generated StreamCuts are only valid if the current event is in the same segment as initialStreamCut.
     * This assumption breaks if scaling or (possibly) when the segment owner changes due to a failure.
     */
    public ExtendedEventPointer(EventReadMetadata eventReadMetadata, StreamCut initialStreamCut) {
        final EventPointer eventPointer = eventReadMetadata.getEventPointer();
        ByteBuffer buf = eventReadMetadata.getEventPointer().toBytes();
        eventPointerBytes = new byte[buf.remaining()];
        buf.get(eventPointerBytes);
        final EventPointerExposed ep = new EventPointerExposed(eventReadMetadata.getEventPointer().asImpl());
        final StreamCutInternal sc = initialStreamCut.asImpl();
        final Map<Segment, Long> positions = new HashMap<>(sc.getPositions());
        positions.put(ep.getSegment(), ep.getEventStartOffset());
        final StreamCut startStreamCut = new StreamCutImpl(sc.getStream(), positions);
        startStreamCutText = startStreamCut.asText();
        positions.put(ep.getSegment(), ep.getEventStartOffset() + ep.getEventLength());
        final StreamCut endStreamCut = new StreamCutImpl(sc.getStream(), positions);
        endStreamCutText = endStreamCut.asText();
        description = "ExtendedEventPointer{" +
                "eventPointer=" + eventPointer +
                ", startStreamCut=" + startStreamCut +
                ", endStreamCut=" + endStreamCut +
                '}';
    }

    public byte[] getEventPointerBytes() {
        return eventPointerBytes;
    }

    public String getStartStreamCutText() {
        return startStreamCutText;
    }

    public String getEndStreamCutText() {
        return endStreamCutText;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }
}
