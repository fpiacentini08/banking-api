package com.example.banking.eventsourcing;

/** Latest-only aggregate snapshot. revision guards against stale state shapes. */
public record Snapshot(String aggregateId, long sequenceNr, int revision, String payload) {
}
