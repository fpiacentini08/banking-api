package com.example.banking.eventsourcing.event;

/** Supplies the {@code eventId} stamped on a {@link SerializedEvent}. Injected like the kernel
 *  clock so serialization stays deterministic under test. */
public interface EventIdGenerator {
    String next();
}
