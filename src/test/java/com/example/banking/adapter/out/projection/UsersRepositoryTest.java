package com.example.banking.adapter.out.projection;

import com.example.banking.infra.FullContextTest;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@FullContextTest
class UsersRepositoryTest {

    @Autowired DSLContext dsl;

    @Test
    void upsertRoundTripsNameEmailAndRegisteredAt() {
        UsersRepository repository = new UsersRepository(dsl);
        String userId = "user-repo-roundtrip";
        // Microsecond precision: fits TIMESTAMP(6) exactly, so no truncation flakiness.
        Instant registeredAt = Instant.parse("2026-07-24T10:15:30.123456Z");

        repository.upsert(userId, "Ada Lovelace", "ada@example.com", registeredAt);

        Record row = dsl.select(field("name"), field("email"), field("registered_at"))
                .from(table("users"))
                .where(field("user_id").eq(userId))
                .fetchOne();
        assertThat(row).isNotNull();
        assertThat(row.get("name", String.class)).isEqualTo("Ada Lovelace");
        assertThat(row.get("email", String.class)).isEqualTo("ada@example.com");
        assertThat(row.get("registered_at", LocalDateTime.class))
                .isEqualTo(LocalDateTime.ofInstant(registeredAt, ZoneOffset.UTC));
    }

    @Test
    void reUpsertOfSameUserIdUpdatesInPlace() {
        UsersRepository repository = new UsersRepository(dsl);
        String userId = "user-repo-reupsert";
        Instant registeredAt = Instant.parse("2026-07-24T10:15:30.123456Z");

        repository.upsert(userId, "Ada Lovelace", "ada@example.com", registeredAt);
        repository.upsert(userId, "Grace Hopper", "grace@example.com", registeredAt);

        assertThat(dsl.fetchCount(table("users"), field("user_id").eq(userId))).isEqualTo(1);
        Record row = dsl.select(field("name"), field("email"))
                .from(table("users"))
                .where(field("user_id").eq(userId))
                .fetchOne();
        assertThat(row).isNotNull();
        assertThat(row.get("name", String.class)).isEqualTo("Grace Hopper");
        assertThat(row.get("email", String.class)).isEqualTo("grace@example.com");
    }
}
