package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;

public class EventPointerExposed extends EventPointerImpl {
    public EventPointerExposed(EventPointerInternal eventPointer) {
        super(eventPointer.getSegment(), eventPointer.getEventStartOffset(), eventPointer.getEventLength());
    }

    @Override
    public Segment getSegment() {
        return super.getSegment();
    }

    @Override
    public long getEventStartOffset() {
        return super.getEventStartOffset();
    }

    @Override
    public int getEventLength() {
        return super.getEventLength();
    }
}
