package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.ConcurrencyConflict;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.util.List;

public final class JdbcEventStore implements EventStore {

    private static final RowMapper<StoredEvent> ROW_MAPPER = (rs, rowNum) -> new StoredEvent(
            rs.getLong("global_position"),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getLong("sequence_nr"),
            new SerializedEvent(
                    rs.getString("event_id"),
                    rs.getString("event_type"),
                    rs.getInt("revision"),
                    rs.getString("payload"),
                    rs.getString("metadata"),
                    rs.getTimestamp("occurred_at").toInstant()));

    private final JdbcTemplate jdbc;
    private final TransactionalRunner tx;

    public JdbcEventStore(JdbcTemplate jdbc, TransactionalRunner tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @Override
    public void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        tx.inTransaction(() -> {
            long firstPosition = jdbc.queryForObject(
                    "SELECT next_position FROM event_store_sequence WHERE id = 1 FOR UPDATE", Long.class);
            try {
                for (int i = 0; i < events.size(); i++) {
                    SerializedEvent e = events.get(i);
                    jdbc.update("""
                                    INSERT INTO domain_event (global_position, aggregate_type, aggregate_id,
                                        sequence_nr, event_id, event_type, revision, payload, metadata, occurred_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            firstPosition + i, aggregateType, aggregateId, expectedVersion + 1 + i,
                            e.eventId(), e.eventType(), e.revision(), e.payload(), e.metadata(),
                            Timestamp.from(e.occurredAt()));
                }
            } catch (DuplicateKeyException conflict) {
                throw new ConcurrencyConflict(aggregateId, expectedVersion);
            }
            jdbc.update("UPDATE event_store_sequence SET next_position = ? WHERE id = 1",
                    firstPosition + events.size());
        });
    }

    @Override
    public List<StoredEvent> readStream(String aggregateId, long afterSequenceNr) {
        return jdbc.query("""
                        SELECT * FROM domain_event
                        WHERE aggregate_id = ? AND sequence_nr > ?
                        ORDER BY sequence_nr""",
                ROW_MAPPER, aggregateId, afterSequenceNr);
    }

    @Override
    public List<StoredEvent> readAllAfter(long globalPosition, int limit) {
        return jdbc.query("""
                        SELECT * FROM domain_event
                        WHERE global_position > ?
                        ORDER BY global_position
                        LIMIT ?""",
                ROW_MAPPER, globalPosition, limit);
    }
}
