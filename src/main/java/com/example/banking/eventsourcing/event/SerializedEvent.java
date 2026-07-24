package com.example.banking.eventsourcing.event;

import java.time.Instant;

/** A domain event in storage form: JSON payload plus type/revision for upcasting. */
public record SerializedEvent(
        String eventId,
        String eventType,
        int revision,
        String payload,
        String metadata,
        Instant occurredAt) {
}
