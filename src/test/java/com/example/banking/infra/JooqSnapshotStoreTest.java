package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.snapshot.JooqSnapshotStore;
import com.example.banking.eventsourcing.snapshot.Snapshot;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class JooqSnapshotStoreTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;

    SnapshotStore store;

    @BeforeEach
    void setUp() {
        store = new JooqSnapshotStore(dsl);
        jdbc.update("DELETE FROM snapshot");
    }

    @Test
    void missingSnapshotIsEmpty() {
        assertThat(store.load("a-none")).isEmpty();
    }

    @Test
    void savesAndOverwritesLatestOnly() {
        store.save(new Snapshot("a-1", 99, 1, "{\"value\":10}"));
        store.save(new Snapshot("a-1", 199, 1, "{\"value\":20}"));

        assertThat(store.load("a-1")).hasValueSatisfying(s -> {
            assertThat(s.sequenceNr()).isEqualTo(199);
            assertThat(s.payload()).contains("20");
        });
    }
}
