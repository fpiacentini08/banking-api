package com.example.banking.eventsourcing;

import io.vavr.control.Either;

import java.util.List;
import java.util.Map;

/**
 * Executes a command against an event-sourced aggregate: load (snapshot + replay), decide,
 * append with optimistic locking, retry on conflict, snapshot past the threshold.
 */
public final class EventSourcingRepository<S, C, E> {

    private record Loaded<S>(S state, long version) {}

    private final AggregateBehaviour<S, C, E> behaviour;
    private final EventStore eventStore;
    private final SnapshotStore snapshotStore;
    private final EventSerializer eventSerializer;
    private final PayloadCodec codec;
    private final Class<S> stateType;
    private final int stateRevision;
    private final int snapshotThreshold;
    private final int maxAttempts;

    public EventSourcingRepository(AggregateBehaviour<S, C, E> behaviour, EventStore eventStore,
                                   SnapshotStore snapshotStore, EventSerializer eventSerializer,
                                   PayloadCodec codec, Class<S> stateType, int stateRevision,
                                   int snapshotThreshold, int maxAttempts) {
        this.behaviour = behaviour;
        this.eventStore = eventStore;
        this.snapshotStore = snapshotStore;
        this.eventSerializer = eventSerializer;
        this.codec = codec;
        this.stateType = stateType;
        this.stateRevision = stateRevision;
        this.snapshotThreshold = snapshotThreshold;
        this.maxAttempts = maxAttempts;
    }

    public Either<DomainError, CommittedEvents> execute(String aggregateId, C command) {
        ConcurrencyConflict lastConflict = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Loaded<S> loaded = load(aggregateId);
            Either<DomainError, List<E>> decision = behaviour.decide(loaded.state(), command);
            if (decision.isLeft()) {
                return Either.left(decision.getLeft());
            }
            List<E> newEvents = decision.get();
            if (newEvents.isEmpty()) {
                return Either.right(new CommittedEvents(aggregateId, loaded.version(), List.of()));
            }
            try {
                eventStore.append(behaviour.aggregateType(), aggregateId, loaded.version(),
                        newEvents.stream().map(e -> eventSerializer.serialize(e, Map.of())).toList());
            } catch (ConcurrencyConflict conflict) {
                lastConflict = conflict;
                continue;
            }
            long newVersion = loaded.version() + newEvents.size();
            maybeSnapshot(aggregateId, loaded, newEvents, newVersion);
            return Either.right(new CommittedEvents(aggregateId, newVersion, List.copyOf(newEvents)));
        }
        throw lastConflict;
    }

    private Loaded<S> load(String aggregateId) {
        S state = behaviour.initial();
        long version = -1;
        Snapshot snapshot = snapshotStore.load(aggregateId)
                .filter(s -> s.revision() == stateRevision)
                .orElse(null);
        if (snapshot != null) {
            state = codec.decode(snapshot.payload(), stateType);
            version = snapshot.sequenceNr();
        }
        for (StoredEvent stored : eventStore.readStream(aggregateId, version)) {
            @SuppressWarnings("unchecked")
            E event = (E) eventSerializer.deserialize(stored.event());
            state = behaviour.evolve(state, event);
            version = stored.sequenceNr();
        }
        return new Loaded<>(state, version);
    }

    private void maybeSnapshot(String aggregateId, Loaded<S> loaded, List<E> newEvents, long newVersion) {
        if ((loaded.version() + 1) / snapshotThreshold == (newVersion + 1) / snapshotThreshold) {
            return;  // threshold not crossed
        }
        S state = loaded.state();
        for (E event : newEvents) {
            state = behaviour.evolve(state, event);
        }
        snapshotStore.save(new Snapshot(aggregateId, newVersion, stateRevision, codec.encode(state)));
    }
}
