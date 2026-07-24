package com.example.banking.infra;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class MySqlFlywayTest {

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void flywayHistoryTableExists() {
        Integer migrations = jdbc.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history", Integer.class);
        assertThat(migrations).isGreaterThanOrEqualTo(1);
    }
}
