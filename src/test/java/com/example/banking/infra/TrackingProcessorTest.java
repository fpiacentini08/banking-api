package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.JdbcEventStore;
import com.example.banking.adapter.out.eventstore.JdbcTokenStore;
import com.example.banking.adapter.out.eventstore.SpringTransactionalRunner;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import com.example.banking.eventsourcing.TokenStore;
import com.example.banking.eventsourcing.TrackingProcessor;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest
@Import(ContainersConfig.class)
class TrackingProcessorTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager txManager;

    EventStore eventStore;
    TokenStore tokenStore;
    TransactionalRunner tx;
    List<StoredEvent> seen;

    @BeforeEach
    void setUp() {
        tx = new SpringTransactionalRunner(new TransactionTemplate(txManager));
        eventStore = new JdbcEventStore(jdbc, tx);
        tokenStore = new JdbcTokenStore(jdbc);
        seen = new CopyOnWriteArrayList<>();
        jdbc.update("DELETE FROM domain_event");
        jdbc.update("DELETE FROM tracking_token");
        jdbc.update("UPDATE event_store_sequence SET next_position = 1");
    }

    private TrackingProcessor processor(String name) {
        return new TrackingProcessor(name, eventStore, tokenStore, tx, seen::add,
                2, Duration.ofMillis(50));
    }

    private void appendEvents(String aggregateId, int count) {
        for (int i = 0; i < count; i++) {
            eventStore.append("Counter", aggregateId, i - 1, List.of(new SerializedEvent(
                    UUID.randomUUID().toString(), "Incremented", 1, "{\"by\":1}", "{}", Instant.now())));
        }
    }

    @Test
    void processesBatchesAndAdvancesToken() {
        appendEvents("p-1", 5);
        TrackingProcessor processor = processor("test-proc");

        assertThat(processor.processOnce()).isEqualTo(2);
        assertThat(processor.processOnce()).isEqualTo(2);
        assertThat(processor.processOnce()).isEqualTo(1);
        assertThat(processor.processOnce()).isEqualTo(0);

        assertThat(seen).hasSize(5);
        assertThat(seen).extracting(StoredEvent::globalPosition).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(tokenStore.load("test-proc")).isEqualTo(5);
    }

    @Test
    void resumesFromPersistedTokenAfterRestart() {
        appendEvents("p-2", 3);
        processor("resume-proc").processOnce();  // applies 2, token = 2

        TrackingProcessor fresh = processor("resume-proc");
        fresh.processOnce();

        assertThat(seen).hasSize(3);
        assertThat(tokenStore.load("resume-proc")).isEqualTo(3);
    }

    @Test
    void handlerFailureRollsBackTokenAndBatchIsRetried() {
        appendEvents("p-3", 1);
        TrackingProcessor failing = new TrackingProcessor("fail-proc", eventStore, tokenStore, tx,
                event -> { throw new IllegalStateException("projection down"); },
                10, Duration.ofMillis(50));

        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(failing::processOnce);
        assertThat(tokenStore.load("fail-proc")).isZero();

        processor("fail-proc").processOnce();  // same name, healthy handler: event is redelivered
        assertThat(seen).hasSize(1);
    }

    @Test
    void resetReplaysFromStartOfStream() {
        appendEvents("p-4", 2);
        TrackingProcessor processor = processor("rebuild-proc");
        processor.processOnce();
        assertThat(seen).hasSize(2);

        tokenStore.reset("rebuild-proc");
        processor.processOnce();

        assertThat(seen).hasSize(4);  // replayed both events
    }
}
