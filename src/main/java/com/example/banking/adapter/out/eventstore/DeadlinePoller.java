package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import com.example.banking.eventsourcing.SagaManager;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Delivers due deadlines to their saga manager and deletes them, one transaction per poll. */
public final class DeadlinePoller {

    private record DueDeadline(String deadlineId, String sagaType, String sagaId,
                               String payloadType, String payload) {}

    private final JdbcTemplate jdbc;
    private final TransactionalRunner tx;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Map<String, SagaManager<?>> managersBySagaType;
    private final Clock clock;
    private final Duration pollInterval;
    private volatile boolean running;
    private Thread loop;

    public DeadlinePoller(JdbcTemplate jdbc, TransactionalRunner tx, PayloadCodec codec,
                          EventTypeRegistry registry, Map<String, SagaManager<?>> managersBySagaType,
                          Clock clock, Duration pollInterval) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
        this.registry = registry;
        this.managersBySagaType = Map.copyOf(managersBySagaType);
        this.clock = clock;
        this.pollInterval = pollInterval;
    }

    /** One transactional pass: deliver every due deadline, delete it, return the count. */
    public int pollOnce() {
        AtomicInteger delivered = new AtomicInteger();
        tx.inTransaction(() -> {
            List<DueDeadline> due = jdbc.query("""
                            SELECT deadline_id, saga_type, saga_id, payload_type, payload
                            FROM deadline WHERE due_at <= ?
                            ORDER BY due_at LIMIT 100 FOR UPDATE""",
                    (rs, rowNum) -> new DueDeadline(rs.getString("deadline_id"), rs.getString("saga_type"),
                            rs.getString("saga_id"), rs.getString("payload_type"), rs.getString("payload")),
                    Timestamp.from(clock.instant()));
            for (DueDeadline deadline : due) {
                SagaManager<?> manager = managersBySagaType.get(deadline.sagaType());
                if (manager != null) {
                    Object payload = codec.decode(deadline.payload(),
                            registry.byName(deadline.payloadType()).type());
                    manager.handleDeadline(deadline.sagaId(), payload);
                }
                jdbc.update("DELETE FROM deadline WHERE deadline_id = ?", deadline.deadlineId());
                delivered.incrementAndGet();
            }
        });
        return delivered.get();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = new Thread(this::runLoop, "deadline-poller");
        loop.setDaemon(true);
        loop.start();
    }

    public synchronized void stop() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    private void runLoop() {
        while (running) {
            try {
                pollOnce();
            } catch (RuntimeException e) {
                // roll back; due deadlines stay in the table and are retried next pass
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
