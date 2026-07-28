package com.example.banking.eventsourcing.saga;

/** Supplies the id of a newly started {@link SagaInstance}. Injected like the kernel clock so saga
 *  start stays deterministic under test. */
public interface SagaIdGenerator {
    String next();
}
