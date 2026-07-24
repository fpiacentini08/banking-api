package com.example.banking.eventsourcing.aggregate;

import java.util.List;

/** The successful outcome of a command: the events appended and the stream's new head. */
public record CommittedEvents(String aggregateId, long lastSequenceNr, List<Object> events) {
}
