package com.example.banking.adapter.out.projection;

import com.example.banking.adapter.out.eventstore.processor.JooqTokenStore;
import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.adapter.out.eventstore.store.JooqEventStore;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;
import com.example.banking.domain.user.UserId;
import com.example.banking.domain.user.UserRegistered;
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

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class UsersProjectionTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;
    @Autowired ObjectMapper mapper;

    @Test
    void projectsUserRegisteredIntoUsersRow() {
        TransactionalRunner tx = new SpringTransactionalRunner(new TransactionTemplate(txManager));
        EventStore eventStore = new JooqEventStore(dsl, tx);
        TokenStore tokenStore = new JooqTokenStore(dsl);

        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("UserRegistered", 1, UserRegistered.class);
        EventSerializer serializer =
                new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), Clock.systemUTC(),
                        new SequentialIds("users-projection-event"));

        UserId id = new UserId("user-users-projection");
        SerializedEvent event =
                serializer.serialize(new UserRegistered(id, "Ada Lovelace", "ada@example.com"), Map.of());
        eventStore.append("User", id.value(), -1, List.of(event));

        UsersProjection projection = new UsersProjection(new UsersRepository(dsl), serializer);
        TrackingProcessor processor = new TrackingProcessor(
                "users-projection-test", eventStore, tokenStore, tx, projection, 1000, Duration.ofMillis(50));
        int applied;
        do {
            applied = processor.processOnce();
        } while (applied > 0);

        Map<String, Object> row =
                jdbc.queryForMap("SELECT name, email FROM users WHERE user_id = ?", id.value());
        assertThat(row).containsEntry("name", "Ada Lovelace").containsEntry("email", "ada@example.com");
    }
}
