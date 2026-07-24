package com.example.banking.eventsourcing.saga;

/** Persisted saga state. statePayload is the codec-encoded saga state. */
public record SagaInstance(String sagaId, String sagaType, String statePayload, boolean terminal) {
}
