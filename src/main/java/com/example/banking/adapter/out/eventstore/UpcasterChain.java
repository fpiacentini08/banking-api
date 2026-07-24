package com.example.banking.adapter.out.eventstore;

import tools.jackson.databind.JsonNode;

import java.util.List;

/** Runs registered upcasters until the payload reaches the current revision. */
public final class UpcasterChain {

    private final List<Upcaster> upcasters;

    public UpcasterChain(List<Upcaster> upcasters) {
        this.upcasters = List.copyOf(upcasters);
    }

    public JsonNode upcast(String eventType, int fromRevision, int toRevision, JsonNode payload) {
        JsonNode current = payload;
        for (int revision = fromRevision; revision < toRevision; revision++) {
            current = step(eventType, revision, current);
        }
        return current;
    }

    private JsonNode step(String eventType, int fromRevision, JsonNode payload) {
        return upcasters.stream()
                .filter(u -> u.eventType().equals(eventType) && u.fromRevision() == fromRevision)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no upcaster for " + eventType + " revision " + fromRevision))
                .upcast(payload);
    }
}
