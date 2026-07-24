package com.example.banking.eventsourcing.event;

import java.util.List;

/**
 * Append-only event store. Sequences are 0-based per stream; a new stream is appended with
 * {@code expectedVersion = -1}. Global positions are strictly increasing, gap-free, and follow
 * commit order.
 */
public interface EventStore {

    /** @throws ConcurrencyConflict when expectedVersion is stale */
    void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events);

    List<StoredEvent> readStream(String aggregateId, long afterSequenceNr);

    List<StoredEvent> readAllAfter(long globalPosition, int limit);
}
