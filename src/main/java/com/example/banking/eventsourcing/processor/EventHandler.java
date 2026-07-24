package com.example.banking.eventsourcing.processor;

import com.example.banking.eventsourcing.event.StoredEvent;

/** A projection/relay/saga hook invoked for each committed event, in global order. */
public interface EventHandler {
    void handle(StoredEvent event);
}
