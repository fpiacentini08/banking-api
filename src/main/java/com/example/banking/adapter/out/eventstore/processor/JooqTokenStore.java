package com.example.banking.adapter.out.eventstore.processor;

import com.example.banking.eventsourcing.processor.TokenStore;
import org.jooq.DSLContext;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public final class JooqTokenStore implements TokenStore {

    private final DSLContext dsl;

    public JooqTokenStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public long load(String processorName) {
        dsl.insertInto(table("tracking_token"))
                .columns(field("processor_name"), field("position"))
                .values(processorName, 0L)
                .onDuplicateKeyIgnore()
                .execute();
        return dsl.select(field("position", Long.class))
                .from(table("tracking_token"))
                .where(field("processor_name").eq(processorName))
                .forUpdate()
                .fetchOne(field("position", Long.class));
    }

    @Override
    public void save(String processorName, long position) {
        dsl.update(table("tracking_token"))
                .set(field("position"), position)
                .where(field("processor_name").eq(processorName))
                .execute();
    }

    @Override
    public void reset(String processorName) {
        dsl.update(table("tracking_token"))
                .set(field("position"), 0L)
                .where(field("processor_name").eq(processorName))
                .execute();
    }
}
