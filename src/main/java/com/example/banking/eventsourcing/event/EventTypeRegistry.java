package com.example.banking.eventsourcing.event;

import java.util.HashMap;
import java.util.Map;

/** Explicit event-type registration: logical name + current revision per event class. */
public final class EventTypeRegistry {

    public record EventType(String name, int currentRevision, Class<?> type) {}

    private final Map<Class<?>, EventType> byClass = new HashMap<>();
    private final Map<String, EventType> byName = new HashMap<>();

    public void register(String name, int currentRevision, Class<?> type) {
        EventType eventType = new EventType(name, currentRevision, type);
        byClass.put(type, eventType);
        byName.put(name, eventType);
    }

    public EventType byClass(Class<?> type) {
        EventType found = byClass.get(type);
        if (found == null) {
            throw new IllegalArgumentException("event class not registered: " + type.getName());
        }
        return found;
    }

    public EventType byName(String name) {
        EventType found = byName.get(name);
        if (found == null) {
            throw new IllegalArgumentException("event type not registered: " + name);
        }
        return found;
    }
}
