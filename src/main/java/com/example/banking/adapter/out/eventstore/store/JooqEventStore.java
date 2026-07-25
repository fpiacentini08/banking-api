package com.example.banking.adapter.out.eventstore.store;

import com.example.banking.eventsourcing.event.ConcurrencyConflict;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.dao.DuplicateKeyException;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class JooqEventStore implements EventStore {

    private final DSLContext dsl;
    private final TransactionalRunner tx;

    public JooqEventStore(DSLContext dsl, TransactionalRunner tx) {
        this.dsl = dsl;
        this.tx = tx;
    }

    @Override
    public void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        tx.inTransaction(() -> {
            long firstPosition = dsl.select(field("next_position", Long.class))
                    .from(table("event_store_sequence"))
                    .where(field("id").eq(1))
                    .forUpdate()
                    .fetchOne(field("next_position", Long.class));
            try {
                for (int i = 0; i < events.size(); i++) {
                    SerializedEvent e = events.get(i);
                    dsl.insertInto(table("domain_event"))
                            .columns(field("global_position"), field("aggregate_type"), field("aggregate_id"),
                                    field("sequence_nr"), field("event_id"), field("event_type"),
                                    field("revision"), field("payload"), field("metadata"), field("occurred_at"))
                            .values(firstPosition + i, aggregateType, aggregateId, expectedVersion + 1 + i,
                                    e.eventId(), e.eventType(), e.revision(), e.payload(), e.metadata(),
                                    LocalDateTime.ofInstant(e.occurredAt(), ZoneOffset.UTC))
                            .execute();
                }
            } catch (DuplicateKeyException conflict) {
                throw new ConcurrencyConflict(aggregateId, expectedVersion);
            }
            dsl.update(table("event_store_sequence"))
                    .set(field("next_position"), firstPosition + events.size())
                    .where(field("id").eq(1))
                    .execute();
        });
    }

    @Override
    public List<StoredEvent> readStream(String aggregateId, long afterSequenceNr) {
        return dsl.select().from(table("domain_event"))
                .where(field("aggregate_id").eq(aggregateId).and(field("sequence_nr").gt(afterSequenceNr)))
                .orderBy(field("sequence_nr"))
                .fetch(JooqEventStore::toStoredEvent);
    }

    @Override
    public List<StoredEvent> readAllAfter(long globalPosition, int limit) {
        return dsl.select().from(table("domain_event"))
                .where(field("global_position").gt(globalPosition))
                .orderBy(field("global_position"))
                .limit(limit)
                .fetch(JooqEventStore::toStoredEvent);
    }

    private static StoredEvent toStoredEvent(Record r) {
        return new StoredEvent(
                r.get("global_position", Long.class),
                r.get("aggregate_type", String.class),
                r.get("aggregate_id", String.class),
                r.get("sequence_nr", Long.class),
                new SerializedEvent(
                        r.get("event_id", String.class),
                        r.get("event_type", String.class),
                        r.get("revision", Integer.class),
                        r.get("payload", String.class),
                        r.get("metadata", String.class),
                        r.get("occurred_at", LocalDateTime.class).toInstant(ZoneOffset.UTC)));
    }
}
