package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.event.EventIdGenerator;
import com.example.banking.eventsourcing.saga.SagaIdGenerator;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic replacement for the UUID generators in tests: {@code <prefix>-1}, {@code <prefix>-2},
 *  … A per-test-class prefix keeps ids unique in the shared MySQL container. */
public final class SequentialIds implements EventIdGenerator, SagaIdGenerator {

    private final String prefix;
    private final AtomicLong counter = new AtomicLong();

    public SequentialIds(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String next() {
        return prefix + "-" + counter.incrementAndGet();
    }
}
