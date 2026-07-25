package com.example.banking.adapter.out.eventstore.saga;

import org.jooq.DSLContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/** The DB half of the deadline poller: reads due deadlines (locking them) and deletes them. */
public final class DeadlineRepository {

    public record DueDeadline(String deadlineId, String sagaType, String sagaId,
                              String payloadType, String payload) {}

    private final DSLContext dsl;

    public DeadlineRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public List<DueDeadline> findDue(Instant now, int limit) {
        return dsl.select(field("deadline_id"), field("saga_type"), field("saga_id"),
                        field("payload_type"), field("payload"))
                .from(table("deadline"))
                .where(field("due_at").le(LocalDateTime.ofInstant(now, ZoneOffset.UTC)))
                .orderBy(field("due_at"))
                .limit(limit)
                .forUpdate()
                .fetch(r -> new DueDeadline(
                        r.get("deadline_id", String.class),
                        r.get("saga_type", String.class),
                        r.get("saga_id", String.class),
                        r.get("payload_type", String.class),
                        r.get("payload", String.class)));
    }

    public void delete(String deadlineId) {
        dsl.deleteFrom(table("deadline"))
                .where(field("deadline_id").eq(deadlineId))
                .execute();
    }
}
