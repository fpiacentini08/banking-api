package com.example.banking.adapter.out.eventstore.saga;

import com.example.banking.eventsourcing.saga.SagaInstance;
import com.example.banking.eventsourcing.saga.SagaStore;
import org.jooq.DSLContext;
import org.jooq.Record;

import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class JooqSagaStore implements SagaStore {

    private final DSLContext dsl;

    public JooqSagaStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public Optional<SagaInstance> findByAssociation(String sagaType, String associationKey) {
        return dsl.select(field("s.saga_id").as("saga_id"), field("s.saga_type").as("saga_type"),
                        field("s.state").as("state"), field("s.terminal").as("terminal"))
                .from(table("saga_instance").as("s"))
                .join(table("saga_association").as("a"))
                .on(field("a.saga_id", String.class).eq(field("s.saga_id", String.class)))
                .where(field("a.saga_type").eq(sagaType).and(field("a.association_key").eq(associationKey)))
                .fetchOptional(JooqSagaStore::toSaga);
    }

    @Override
    public Optional<SagaInstance> findById(String sagaId) {
        return dsl.select(field("saga_id"), field("saga_type"), field("state"), field("terminal"))
                .from(table("saga_instance"))
                .where(field("saga_id").eq(sagaId))
                .fetchOptional(JooqSagaStore::toSaga);
    }

    @Override
    public void insert(SagaInstance saga, String associationKey) {
        dsl.insertInto(table("saga_instance"))
                .columns(field("saga_id"), field("saga_type"), field("state"), field("terminal"))
                .values(saga.sagaId(), saga.sagaType(), saga.statePayload(), saga.terminal())
                .execute();
        dsl.insertInto(table("saga_association"))
                .columns(field("saga_type"), field("association_key"), field("saga_id"))
                .values(saga.sagaType(), associationKey, saga.sagaId())
                .execute();
    }

    @Override
    public void save(SagaInstance saga) {
        dsl.update(table("saga_instance"))
                .set(field("state"), saga.statePayload())
                .set(field("terminal"), saga.terminal())
                .where(field("saga_id").eq(saga.sagaId()))
                .execute();
    }

    private static SagaInstance toSaga(Record r) {
        return new SagaInstance(
                r.get("saga_id", String.class),
                r.get("saga_type", String.class),
                r.get("state", String.class),
                r.get("terminal", Boolean.class));
    }
}
