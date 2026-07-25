package com.example.banking.adapter.out.projection;

import com.example.banking.application.AccountBalanceView;
import com.example.banking.application.AccountBalances;
import org.jooq.DSLContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/** The only class that reads/writes the account_balance read model. jOOQ DSL-only. */
public final class AccountBalanceRepository implements AccountBalances {

    private final DSLContext dsl;

    public AccountBalanceRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public Optional<AccountBalanceView> find(String accountId) {
        return dsl.select(field("owner_id"), field("balance"), field("version"), field("updated_at"))
                .from(table("account_balance"))
                .where(field("account_id").eq(accountId))
                .fetchOptional()
                .map(r -> new AccountBalanceView(
                        r.get("owner_id", String.class),
                        r.get("balance", Long.class),
                        r.get("version", Long.class),
                        r.get("updated_at", LocalDateTime.class).toInstant(ZoneOffset.UTC)));
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
