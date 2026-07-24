package com.example.banking.eventsourcing.event;

import java.util.Map;

/** Turns domain events into storage form and back, applying upcasters on read. */
public interface EventSerializer {
    SerializedEvent serialize(Object event, Map<String, String> metadata);
    Object deserialize(SerializedEvent event);
}
