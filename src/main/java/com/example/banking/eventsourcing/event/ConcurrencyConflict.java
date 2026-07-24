package com.example.banking.eventsourcing.event;

/** Optimistic-locking failure: another command appended to the stream first. */
public final class ConcurrencyConflict extends RuntimeException {

    public ConcurrencyConflict(String aggregateId, long expectedVersion) {
        super("concurrent append to aggregate " + aggregateId + " at expected version " + expectedVersion);
    }
}
