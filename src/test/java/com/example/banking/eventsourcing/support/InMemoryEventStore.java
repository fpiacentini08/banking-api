package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.ConcurrencyConflict;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;

import java.util.ArrayList;
import java.util.List;

/** Test fake. Set failNextAppends > 0 to simulate optimistic-lock conflicts. */
public final class InMemoryEventStore implements EventStore {

    private final List<StoredEvent> events = new ArrayList<>();
    public int failNextAppends = 0;
    public long lastReadAfter = Long.MIN_VALUE;

    @Override
    public synchronized void append(String aggregateType, String aggregateId,
                                    long expectedVersion, List<SerializedEvent> newEvents) {
        if (failNextAppends > 0) {
            failNextAppends--;
            throw new ConcurrencyConflict(aggregateId, expectedVersion);
        }
        long current = events.stream()
                .filter(e -> e.aggregateId().equals(aggregateId))
                .mapToLong(StoredEvent::sequenceNr).max().orElse(-1);
        if (current != expectedVersion) {
            throw new ConcurrencyConflict(aggregateId, expectedVersion);
        }
        for (int i = 0; i < newEvents.size(); i++) {
            events.add(new StoredEvent(events.size() + 1L, aggregateType, aggregateId,
                    expectedVersion + 1 + i, newEvents.get(i)));
        }
    }

    @Override
    public synchronized List<StoredEvent> readStream(String aggregateId, long afterSequenceNr) {
        lastReadAfter = afterSequenceNr;
        return events.stream()
                .filter(e -> e.aggregateId().equals(aggregateId) && e.sequenceNr() > afterSequenceNr)
                .toList();
    }

    @Override
    public synchronized List<StoredEvent> readAllAfter(long globalPosition, int limit) {
        return events.stream()
                .filter(e -> e.globalPosition() > globalPosition)
                .limit(limit)
                .toList();
    }
}
