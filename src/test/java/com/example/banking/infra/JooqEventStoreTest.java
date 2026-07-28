package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.store.JooqEventStore;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;
import com.example.banking.eventsourcing.event.ConcurrencyConflict;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.support.SequentialIds;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest
@Import(ContainersConfig.class)
class JooqEventStoreTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;

    EventStore store;

    @BeforeEach
    void setUp() {
        store = new JooqEventStore(dsl, new SpringTransactionalRunner(new TransactionTemplate(txManager)));
        jdbc.update("DELETE FROM domain_event");
        jdbc.update("UPDATE event_store_sequence SET next_position = 1");
    }

    private static final SequentialIds EVENT_IDS = new SequentialIds("event-store-event");

    private static SerializedEvent event(String type) {
        return new SerializedEvent(EVENT_IDS.next(), type, 1, "{\"by\":1}", "{}", Instant.now());
    }

    @Test
    void appendsAndReadsBackAStreamInOrder() {
        store.append("Counter", "c-1", -1, List.of(event("Incremented"), event("Incremented")));
        store.append("Counter", "c-1", 1, List.of(event("Decremented")));

        List<StoredEvent> stream = store.readStream("c-1", -1);

        assertThat(stream).hasSize(3);
        assertThat(stream).extracting(StoredEvent::sequenceNr).containsExactly(0L, 1L, 2L);
        assertThat(stream.get(2).event().eventType()).isEqualTo("Decremented");
        assertThat(store.readStream("c-1", 1)).hasSize(1);
    }

    @Test
    void staleExpectedVersionRaisesConcurrencyConflict() {
        store.append("Counter", "c-2", -1, List.of(event("Incremented")));

        assertThatExceptionOfType(ConcurrencyConflict.class)
                .isThrownBy(() -> store.append("Counter", "c-2", -1, List.of(event("Incremented"))));
    }

    @Test
    void globalPositionsAreGapFreeAndOrderedAcrossStreams() {
        store.append("Counter", "c-3", -1, List.of(event("Incremented")));
        store.append("Counter", "c-4", -1, List.of(event("Incremented"), event("Incremented")));

        List<StoredEvent> all = store.readAllAfter(0, 10);

        assertThat(all).extracting(StoredEvent::globalPosition).containsExactly(1L, 2L, 3L);
        assertThat(store.readAllAfter(1, 10)).hasSize(2);
        assertThat(store.readAllAfter(3, 10)).isEmpty();
    }

    @Test
    void failedAppendLeavesNoGap() {
        store.append("Counter", "c-5", -1, List.of(event("Incremented")));
        assertThatExceptionOfType(ConcurrencyConflict.class)
                .isThrownBy(() -> store.append("Counter", "c-5", -1, List.of(event("Incremented"))));
        store.append("Counter", "c-6", -1, List.of(event("Incremented")));

        assertThat(store.readAllAfter(0, 10))
                .extracting(StoredEvent::globalPosition).containsExactly(1L, 2L);
    }
}
