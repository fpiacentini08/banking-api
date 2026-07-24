package com.example.banking.adapter.out.eventstore.processor;

import com.example.banking.eventsourcing.processor.EventHandler;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
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

    private static final RowMapper<StoredEvent> ROW_MAPPER = (rs, n) -> new StoredEvent(
            rs.getLong("global_position"),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getLong("sequence_nr"),
            new SerializedEvent(
                    rs.getString("event_id"),
                    rs.getString("event_type"),
                    rs.getInt("revision"),
                    rs.getString("payload"),
                    rs.getString("metadata"),
                    rs.getTimestamp("occurred_at").toInstant()));

    private final JdbcTemplate jdbc;
    private final TransactionalRunner tx;
    private final String processorName;
    private final int batchSize;
    private final EventHandler handler;

    public SegmentedEventProcessor(JdbcTemplate jdbc, TransactionalRunner tx, String processorName,
                                   int batchSize, EventHandler handler) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.processorName = processorName;
        this.batchSize = batchSize;
        this.handler = handler;
    }

    /** Seed one token row per segment (idempotent). Call once before processing starts. */
    public void ensureSegments() {
        for (int segment = 0; segment < SEGMENTS; segment++) {
            jdbc.update("INSERT IGNORE INTO segment_token (processor_name, segment, position) VALUES (?, ?, 0)",
                    processorName, segment);
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
            List<long[]> claim = jdbc.query("""
                            SELECT segment, position FROM segment_token
                            WHERE processor_name = ?
                            ORDER BY position
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED""",
                    (rs, n) -> new long[]{rs.getInt("segment"), rs.getLong("position")}, processorName);
            if (claim.isEmpty()) {
                return;
            }
            int segment = (int) claim.get(0)[0];
            long token = claim.get(0)[1];
            List<StoredEvent> batch = jdbc.query("""
                            SELECT * FROM domain_event
                            WHERE segment = ? AND global_position > ?
                            ORDER BY global_position
                            LIMIT ?""",
                    ROW_MAPPER, segment, token, batchSize);
            for (StoredEvent event : batch) {
                handler.handle(event);
            }
            if (!batch.isEmpty()) {
                jdbc.update("UPDATE segment_token SET position = ? WHERE processor_name = ? AND segment = ?",
                        batch.get(batch.size() - 1).globalPosition(), processorName, segment);
            }
            applied.set(batch.size());
        });
        return applied.get();
    }
}
