package com.example.banking.adapter.out.eventstore.snapshot;

import com.example.banking.eventsourcing.snapshot.Snapshot;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

public final class JdbcSnapshotStore implements SnapshotStore {

    private final JdbcTemplate jdbc;

    public JdbcSnapshotStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Snapshot> load(String aggregateId) {
        return jdbc.query("""
                        SELECT aggregate_id, sequence_nr, revision, payload
                        FROM snapshot WHERE aggregate_id = ?""",
                (rs, rowNum) -> new Snapshot(
                        rs.getString("aggregate_id"),
                        rs.getLong("sequence_nr"),
                        rs.getInt("revision"),
                        rs.getString("payload")),
                aggregateId).stream().findFirst();
    }

    @Override
    public void save(Snapshot snapshot) {
        jdbc.update("""
                        INSERT INTO snapshot (aggregate_id, sequence_nr, revision, payload)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE sequence_nr = VALUES(sequence_nr),
                            revision = VALUES(revision), payload = VALUES(payload)""",
                snapshot.aggregateId(), snapshot.sequenceNr(), snapshot.revision(), snapshot.payload());
    }
}
