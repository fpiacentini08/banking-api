package com.example.banking.adapter.out.eventstore.serialization;

import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.event.SerializedEvent;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

public final class JacksonEventSerializer implements EventSerializer {

    private final ObjectMapper mapper;
    private final EventTypeRegistry registry;
    private final UpcasterChain upcasters;
    private final Clock clock;

    public JacksonEventSerializer(ObjectMapper mapper, EventTypeRegistry registry,
                                  UpcasterChain upcasters, Clock clock) {
        this.mapper = mapper;
        this.registry = registry;
        this.upcasters = upcasters;
        this.clock = clock;
    }

    @Override
    public SerializedEvent serialize(Object event, Map<String, String> metadata) {
        EventTypeRegistry.EventType type = registry.byClass(event.getClass());
        try {
            return new SerializedEvent(
                    UUID.randomUUID().toString(),
                    type.name(),
                    type.currentRevision(),
                    mapper.writeValueAsString(event),
                    mapper.writeValueAsString(metadata),
                    clock.instant());
        } catch (JacksonException e) {
            throw new IllegalArgumentException("cannot serialize " + type.name(), e);
        }
    }

    @Override
    public Object deserialize(SerializedEvent event) {
        EventTypeRegistry.EventType type = registry.byName(event.eventType());
        try {
            JsonNode payload = mapper.readTree(event.payload());
            JsonNode upcast = upcasters.upcast(event.eventType(), event.revision(), type.currentRevision(), payload);
            return mapper.treeToValue(upcast, type.type());
        } catch (JacksonException e) {
            throw new IllegalArgumentException("cannot deserialize " + event.eventType(), e);
        }
    }
}
