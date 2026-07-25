package com.example.banking.adapter.out.projection;

import com.example.banking.domain.user.UserRegistered;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.processor.EventHandler;

/** Read-model projection: filters + deserializes UserRegistered, delegates the write to UsersRepository. */
public final class UsersProjection implements EventHandler {

    private static final String EVENT_TYPE = "UserRegistered";

    private final UsersRepository users;
    private final EventSerializer serializer;

    public UsersProjection(UsersRepository users, EventSerializer serializer) {
        this.users = users;
        this.serializer = serializer;
    }

    @Override
    public void handle(StoredEvent stored) {
        if (!EVENT_TYPE.equals(stored.event().eventType())) {
            return;
        }
        UserRegistered event = (UserRegistered) serializer.deserialize(stored.event());
        users.upsert(event.userId().value(), event.name(), event.email(), stored.event().occurredAt());
    }
}
