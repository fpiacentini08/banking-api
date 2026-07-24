package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.snapshot.Snapshot;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemorySnapshotStore implements SnapshotStore {

    public final Map<String, Snapshot> snapshots = new HashMap<>();

    @Override
    public Optional<Snapshot> load(String aggregateId) {
        return Optional.ofNullable(snapshots.get(aggregateId));
    }

    @Override
    public void save(Snapshot snapshot) {
        snapshots.put(snapshot.aggregateId(), snapshot);
    }
}
