package com.example.banking.adapter.out.projection;

import org.jooq.DSLContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/** The only class that reads/writes the users read model. jOOQ DSL-only. */
public final class UsersRepository {

    private final DSLContext dsl;

    public UsersRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void upsert(String userId, String name, String email, Instant registeredAt) {
        LocalDateTime ts = LocalDateTime.ofInstant(registeredAt, ZoneOffset.UTC);
        dsl.insertInto(table("users"))
                .columns(field("user_id"), field("name"), field("email"), field("registered_at"))
                .values(userId, name, email, ts)
                .onDuplicateKeyUpdate()
                .set(field("name"), name)
                .set(field("email"), email)
                .set(field("registered_at"), ts)
                .execute();
    }
}
