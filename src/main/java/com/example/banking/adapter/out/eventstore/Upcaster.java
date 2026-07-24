package com.example.banking.adapter.out.eventstore;

import tools.jackson.databind.JsonNode;

/** Pure payload migration for one revision step of one event type. */
public interface Upcaster {
    String eventType();
    int fromRevision();
    JsonNode upcast(JsonNode payload);
}
