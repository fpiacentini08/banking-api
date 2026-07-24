package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.TokenStore;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcTokenStore implements TokenStore {

    private final JdbcTemplate jdbc;

    public JdbcTokenStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public long load(String processorName) {
        jdbc.update("INSERT IGNORE INTO tracking_token (processor_name, position) VALUES (?, 0)",
                processorName);
        return jdbc.queryForObject(
                "SELECT position FROM tracking_token WHERE processor_name = ? FOR UPDATE",
                Long.class, processorName);
    }

    @Override
    public void save(String processorName, long position) {
        jdbc.update("UPDATE tracking_token SET position = ? WHERE processor_name = ?",
                position, processorName);
    }

    @Override
    public void reset(String processorName) {
        jdbc.update("UPDATE tracking_token SET position = 0 WHERE processor_name = ?", processorName);
    }
}
