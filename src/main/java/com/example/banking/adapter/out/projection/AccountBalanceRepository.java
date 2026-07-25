package com.example.banking.adapter.out.projection;

import org.jooq.DSLContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/** The only class that reads/writes the account_balance read model. jOOQ DSL-only. */
public final class AccountBalanceRepository {

    private final DSLContext dsl;

    public AccountBalanceRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void upsert(String accountId, String ownerId, long balanceMinor, long version, Instant updatedAt) {
        LocalDateTime ts = LocalDateTime.ofInstant(updatedAt, ZoneOffset.UTC);
        dsl.insertInto(table("account_balance"))
                .columns(field("account_id"), field("owner_id"), field("balance"),
                        field("version"), field("updated_at"))
                .values(accountId, ownerId, balanceMinor, version, ts)
                .onDuplicateKeyUpdate()
                .set(field("owner_id"), ownerId)
                .set(field("balance"), balanceMinor)
                .set(field("version"), version)
                .set(field("updated_at"), ts)
                .execute();
    }
}
