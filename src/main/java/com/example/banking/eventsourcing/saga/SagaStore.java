package com.example.banking.eventsourcing.saga;

import java.util.Optional;

public interface SagaStore {

    Optional<SagaInstance> findByAssociation(String sagaType, String associationKey);

    Optional<SagaInstance> findById(String sagaId);

    void insert(SagaInstance saga, String associationKey);

    void save(SagaInstance saga);
}
