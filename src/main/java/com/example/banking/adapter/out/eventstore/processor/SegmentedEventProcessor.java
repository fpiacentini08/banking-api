package com.example.banking.adapter.out.eventstore.processor;

import com.example.banking.adapter.out.eventstore.processor.SegmentTokenRepository.Claim;
import com.example.banking.eventsourcing.processor.EventHandler;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.common.TransactionalRunner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SPIKE — SKIP LOCKED scale-out for the read side.
 *
 * <p>The stream is partitioned into {@link #SEGMENTS} fixed segments; each event's segment is
 * {@code CRC32(aggregate_id) MOD SEGMENTS}, a stored generated column on {@code domain_event}.
 * Progress is tracked per {@code (processor_name, segment)} in {@code segment_token}. A worker
 * claims exactly one segment with {@code FOR UPDATE SKIP LOCKED}, so at most one worker touches a
 * given segment at a time while other workers process other segments concurrently.
 *
 * <p>Because an aggregate always maps to one segment, per-aggregate event order is preserved even
 * though segments run in parallel across pods. Correctness rests on MySQL alone — no framework, no
 * leader election, no cluster membership. Handler effects and the token update commit in one
 * transaction, so a MySQL-backed projection handler is exactly-once per segment; a crash rolls the
 * batch back and it is redelivered.
 */
public final class SegmentedEventProcessor {

    /** MUST match {@code CRC32(aggregate_id) MOD 32} in migration V7. */
    public static final int SEGMENTS = 32;

    private final SegmentTokenRepository segments;
    private final TransactionalRunner tx;
    private final String processorName;
    private final int batchSize;
    private final EventHandler handler;

    public SegmentedEventProcessor(SegmentTokenRepository segments, TransactionalRunner tx, String processorName,
                                   int batchSize, EventHandler handler) {
        this.segments = segments;
        this.tx = tx;
        this.processorName = processorName;
        this.batchSize = batchSize;
        this.handler = handler;
    }

    /** Seed one token row per segment (idempotent). Call once before processing starts. */
    public void ensureSegments() {
        for (int segment = 0; segment < SEGMENTS; segment++) {
            segments.ensureSegment(processorName, segment);
        }
    }

    /**
     * Claim one available segment (skipping segments another worker holds) and process one batch of
     * its pending events in a single transaction. Returns the number of events applied — 0 when the
     * claimed segment had no pending events, or when every segment is currently held elsewhere.
     */
    public int processOneClaim() {
        AtomicInteger applied = new AtomicInteger();
        tx.inTransaction(() -> {
            Optional<Claim> claim = segments.claim(processorName);
            if (claim.isEmpty()) {
                return;
            }
            int segment = claim.get().segment();
            long token = claim.get().position();
            List<StoredEvent> batch = segments.readBatch(segment, token, batchSize);
            for (StoredEvent event : batch) {
                handler.handle(event);
            }
            if (!batch.isEmpty()) {
                segments.advance(processorName, segment, batch.get(batch.size() - 1).globalPosition());
            }
            applied.set(batch.size());
        });
        return applied.get();
    }
}
