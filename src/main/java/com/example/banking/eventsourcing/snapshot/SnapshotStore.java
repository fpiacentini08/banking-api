package com.example.banking.eventsourcing.snapshot;

import java.util.Optional;

public interface SnapshotStore {
    Optional<Snapshot> load(String aggregateId);
    void save(Snapshot snapshot);
}
