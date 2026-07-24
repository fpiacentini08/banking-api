package com.example.banking.eventsourcing.event;

/** A committed event with its position in the store. */
public record StoredEvent(
        long globalPosition,
        String aggregateType,
        String aggregateId,
        long sequenceNr,
        SerializedEvent event) {
}
