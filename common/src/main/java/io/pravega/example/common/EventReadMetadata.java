package io.pravega.example.common;

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Position;

import java.io.Serializable;

public class EventReadMetadata implements Serializable {
    final private EventPointer eventPointer;
    final private Position position;

    public EventReadMetadata(EventRead<?> eventRead) {
        this.eventPointer = eventRead.getEventPointer();
        this.position = eventRead.getPosition();
    }

    public EventPointer getEventPointer() {
        return eventPointer;
    }

    public Position getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "EventReadMetadata{" +
                "eventPointer=" + eventPointer +
                ", position=" + position +
                '}';
    }

}
