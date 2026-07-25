package com.example.banking.adapter.out.eventstore.processor;

import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.event.StoredEvent;
import org.jooq.DSLContext;
import org.jooq.Record;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * The DB half of the segmented processor: seeds and claims a segment token with
 * {@code FOR UPDATE SKIP LOCKED}, reads a segment's pending events, and advances its token.
 */
public final class SegmentTokenRepository {

    public record Claim(int segment, long position) {}

    private final DSLContext dsl;

    public SegmentTokenRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void ensureSegment(String processorName, int segment) {
        dsl.insertInto(table("segment_token"))
                .columns(field("processor_name"), field("segment"), field("position"))
                .values(processorName, segment, 0L)
                .onDuplicateKeyIgnore()
                .execute();
    }

    public Optional<Claim> claim(String processorName) {
        return dsl.select(field("segment", Integer.class), field("position", Long.class))
                .from(table("segment_token"))
                .where(field("processor_name").eq(processorName))
                .orderBy(field("position"))
                .limit(1)
                .forUpdate()
                .skipLocked()
                .fetchOptional(r -> new Claim(r.get("segment", Integer.class), r.get("position", Long.class)));
    }

    public List<StoredEvent> readBatch(int segment, long afterPosition, int limit) {
        return dsl.select().from(table("domain_event"))
                .where(field("segment").eq(segment).and(field("global_position").gt(afterPosition)))
                .orderBy(field("global_position"))
                .limit(limit)
                .fetch(SegmentTokenRepository::toStoredEvent);
    }

    public void advance(String processorName, int segment, long position) {
        dsl.update(table("segment_token"))
                .set(field("position"), position)
                .where(field("processor_name").eq(processorName).and(field("segment").eq(segment)))
                .execute();
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
