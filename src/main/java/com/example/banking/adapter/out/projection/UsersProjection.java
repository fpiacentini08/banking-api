package com.example.banking.adapter.out.projection;

import com.example.banking.domain.user.UserRegistered;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.processor.EventHandler;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;

/** Read-model projection: one row per registered user, materialized from UserRegistered events. */
public final class UsersProjection implements EventHandler {

    private static final String EVENT_TYPE = "UserRegistered";

    private final JdbcTemplate jdbc;
    private final EventSerializer serializer;

    public UsersProjection(JdbcTemplate jdbc, EventSerializer serializer) {
        this.jdbc = jdbc;
        this.serializer = serializer;
    }

    @Override
    public void handle(StoredEvent stored) {
        if (!EVENT_TYPE.equals(stored.event().eventType())) {
            return;
        }
        UserRegistered event = (UserRegistered) serializer.deserialize(stored.event());
        jdbc.update("""
                        INSERT INTO users (user_id, name, email, registered_at)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email),
                                                registered_at = VALUES(registered_at)
                        """,
                event.userId().value(),
                event.name(),
                event.email(),
                Timestamp.from(stored.event().occurredAt()));
    }
}
