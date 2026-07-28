package com.example.banking.adapter.out.projection;

import com.example.banking.adapter.out.eventstore.processor.JooqTokenStore;
import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.adapter.out.eventstore.store.JooqEventStore;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;
import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.AccountOpened;
import com.example.banking.domain.user.UserId;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.processor.TokenStore;
import com.example.banking.eventsourcing.processor.TrackingProcessor;
import com.example.banking.eventsourcing.support.SequentialIds;
import com.example.banking.infra.FullContextTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class AccountBalanceProjectionTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;
    @Autowired ObjectMapper mapper;

    @Test
    void projectsAccountOpenedIntoAccountBalanceRow() {
        TransactionalRunner tx = new SpringTransactionalRunner(new TransactionTemplate(txManager));
        EventStore eventStore = new JooqEventStore(dsl, tx);
        TokenStore tokenStore = new JooqTokenStore(dsl);

        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("AccountOpened", 1, AccountOpened.class);
        EventSerializer serializer =
                new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), Clock.systemUTC(),
                        new SequentialIds("balance-projection-event"));

        AccountId accountId = new AccountId(UUID.randomUUID().toString());
        UserId ownerId = new UserId(UUID.randomUUID().toString());
        SerializedEvent event = serializer.serialize(new AccountOpened(accountId, ownerId), Map.of());
        eventStore.append("Account", accountId.value(), -1, List.of(event));

        AccountBalanceProjection projection =
                new AccountBalanceProjection(new AccountBalanceRepository(dsl), serializer);
        TrackingProcessor processor = new TrackingProcessor(
                "account-balance-projection-test", eventStore, tokenStore, tx, projection, 1000, Duration.ofMillis(50));
        int applied;
        do {
            applied = processor.processOnce();
        } while (applied > 0);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT owner_id, balance FROM account_balance WHERE account_id = ?", accountId.value());
        assertThat(row).containsEntry("owner_id", ownerId.value());
        assertThat(((Number) row.get("balance")).longValue()).isEqualTo(0L);
    }
}
