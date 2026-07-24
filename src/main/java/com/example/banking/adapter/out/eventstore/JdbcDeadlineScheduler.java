package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.DeadlineScheduler;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;

public final class JdbcDeadlineScheduler implements DeadlineScheduler {

    private final JdbcTemplate jdbc;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Clock clock;

    public JdbcDeadlineScheduler(JdbcTemplate jdbc, PayloadCodec codec,
                                 EventTypeRegistry registry, Clock clock) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.registry = registry;
        this.clock = clock;
    }

    @Override
    public void schedule(String sagaType, String sagaId, DeadlineRequest request) {
        jdbc.update("""
                        INSERT INTO deadline (deadline_id, saga_type, saga_id, due_at, payload_type, payload)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE due_at = VALUES(due_at), payload = VALUES(payload)""",
                request.deadlineId(), sagaType, sagaId,
                Timestamp.from(clock.instant().plus(request.after())),
                registry.byClass(request.payload().getClass()).name(),
                codec.encode(request.payload()));
    }

    @Override
    public void cancel(String deadlineId) {
        jdbc.update("DELETE FROM deadline WHERE deadline_id = ?", deadlineId);
    }
}
