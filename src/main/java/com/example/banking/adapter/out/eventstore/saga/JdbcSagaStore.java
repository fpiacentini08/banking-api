package com.example.banking.adapter.out.eventstore.saga;

import com.example.banking.eventsourcing.saga.SagaInstance;
import com.example.banking.eventsourcing.saga.SagaStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Optional;

public final class JdbcSagaStore implements SagaStore {

    private static final RowMapper<SagaInstance> ROW_MAPPER = (rs, rowNum) -> new SagaInstance(
            rs.getString("saga_id"),
            rs.getString("saga_type"),
            rs.getString("state"),
            rs.getBoolean("terminal"));

    private final JdbcTemplate jdbc;

    public JdbcSagaStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<SagaInstance> findByAssociation(String sagaType, String associationKey) {
        return jdbc.query("""
                        SELECT s.* FROM saga_instance s
                        JOIN saga_association a ON a.saga_id = s.saga_id
                        WHERE a.saga_type = ? AND a.association_key = ?""",
                ROW_MAPPER, sagaType, associationKey).stream().findFirst();
    }

    @Override
    public Optional<SagaInstance> findById(String sagaId) {
        return jdbc.query("SELECT * FROM saga_instance WHERE saga_id = ?", ROW_MAPPER, sagaId)
                .stream().findFirst();
    }

    @Override
    public void insert(SagaInstance saga, String associationKey) {
        jdbc.update("INSERT INTO saga_instance (saga_id, saga_type, state, terminal) VALUES (?, ?, ?, ?)",
                saga.sagaId(), saga.sagaType(), saga.statePayload(), saga.terminal());
        jdbc.update("INSERT INTO saga_association (saga_type, association_key, saga_id) VALUES (?, ?, ?)",
                saga.sagaType(), associationKey, saga.sagaId());
    }

    @Override
    public void save(SagaInstance saga) {
        jdbc.update("UPDATE saga_instance SET state = ?, terminal = ? WHERE saga_id = ?",
                saga.statePayload(), saga.terminal(), saga.sagaId());
    }
}
