package com.example.banking.adapter.out.eventstore.saga;

import com.example.banking.eventsourcing.saga.DeadlineRequest;
import com.example.banking.eventsourcing.saga.DeadlineScheduler;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.common.PayloadCodec;
import org.jooq.DSLContext;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class JooqDeadlineScheduler implements DeadlineScheduler {

    private final DSLContext dsl;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Clock clock;

    public JooqDeadlineScheduler(DSLContext dsl, PayloadCodec codec,
                                 EventTypeRegistry registry, Clock clock) {
        this.dsl = dsl;
        this.codec = codec;
        this.registry = registry;
        this.clock = clock;
    }

    @Override
    public void schedule(String sagaType, String sagaId, DeadlineRequest request) {
        LocalDateTime dueAt = LocalDateTime.ofInstant(clock.instant().plus(request.after()), ZoneOffset.UTC);
        String payloadType = registry.byClass(request.payload().getClass()).name();
        String payload = codec.encode(request.payload());
        dsl.insertInto(table("deadline"))
                .columns(field("deadline_id"), field("saga_type"), field("saga_id"),
                        field("due_at"), field("payload_type"), field("payload"))
                .values(request.deadlineId(), sagaType, sagaId, dueAt, payloadType, payload)
                .onDuplicateKeyUpdate()
                .set(field("due_at"), dueAt)
                .set(field("payload"), payload)
                .execute();
    }

    @Override
    public void cancel(String deadlineId) {
        dsl.deleteFrom(table("deadline"))
                .where(field("deadline_id").eq(deadlineId))
                .execute();
    }
}
