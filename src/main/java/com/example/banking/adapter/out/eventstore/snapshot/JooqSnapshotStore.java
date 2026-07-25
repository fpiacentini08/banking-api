package com.example.banking.adapter.out.eventstore.snapshot;

import com.example.banking.eventsourcing.snapshot.Snapshot;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import org.jooq.DSLContext;

import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class JooqSnapshotStore implements SnapshotStore {

    private final DSLContext dsl;

    public JooqSnapshotStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public Optional<Snapshot> load(String aggregateId) {
        return dsl.select(field("aggregate_id"), field("sequence_nr"), field("revision"), field("payload"))
                .from(table("snapshot"))
                .where(field("aggregate_id").eq(aggregateId))
                .fetchOptional(r -> new Snapshot(
                        r.get("aggregate_id", String.class),
                        r.get("sequence_nr", Long.class),
                        r.get("revision", Integer.class),
                        r.get("payload", String.class)));
    }

    @Override
    public void save(Snapshot snapshot) {
        dsl.insertInto(table("snapshot"))
                .columns(field("aggregate_id"), field("sequence_nr"), field("revision"), field("payload"))
                .values(snapshot.aggregateId(), snapshot.sequenceNr(), snapshot.revision(), snapshot.payload())
                .onDuplicateKeyUpdate()
                .set(field("sequence_nr"), snapshot.sequenceNr())
                .set(field("revision"), snapshot.revision())
                .set(field("payload"), snapshot.payload())
                .execute();
    }
}
