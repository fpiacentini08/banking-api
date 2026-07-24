package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.aggregate.EventSourcingRepository;
import com.example.banking.eventsourcing.common.DomainError;
import com.example.banking.eventsourcing.event.ConcurrencyConflict;
import com.example.banking.eventsourcing.snapshot.Snapshot;

import com.example.banking.eventsourcing.support.CounterBehaviour;
import com.example.banking.eventsourcing.support.InMemoryEventStore;
import com.example.banking.eventsourcing.support.InMemorySnapshotStore;
import io.vavr.control.Either;
import org.junit.jupiter.api.Test;

import static com.example.banking.eventsourcing.support.CounterBehaviour.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class EventSourcingRepositoryTest {

    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

    private EventSourcingRepository<Counter, CounterCommand, CounterEvent> repository(int threshold, int attempts) {
        return new EventSourcingRepository<>(new CounterBehaviour(), eventStore, snapshotStore,
                CounterBehaviour.testSerializer(), CounterBehaviour.testCodec(),
                Counter.class, 1, threshold, attempts);
    }

    @Test
    void rightArm_appendsDecidedEvents() {
        var repo = repository(100, 3);

        Either<DomainError, CommittedEvents> first = repo.execute("c-1", new Increment("c-1", 2));
        Either<DomainError, CommittedEvents> second = repo.execute("c-1", new Decrement("c-1", 1));

        assertThat(first.get().lastSequenceNr()).isEqualTo(0);
        assertThat(second.get().lastSequenceNr()).isEqualTo(1);
        assertThat(second.get().events()).containsExactly(new Decremented(1));
        assertThat(eventStore.readStream("c-1", -1)).hasSize(2);
    }

    @Test
    void leftArm_businessRejectionAppendsNothing() {
        var repo = repository(100, 3);
        repo.execute("c-2", new Increment("c-2", 1));

        Either<DomainError, CommittedEvents> outcome = repo.execute("c-2", new Decrement("c-2", 5));

        assertThat(outcome.getLeft()).isEqualTo(new NegativeCounter(1, 5));
        assertThat(eventStore.readStream("c-2", -1)).hasSize(1);
    }

    @Test
    void retriesOnConflictThenSucceeds() {
        var repo = repository(100, 3);
        eventStore.failNextAppends = 2;

        Either<DomainError, CommittedEvents> outcome = repo.execute("c-3", new Increment("c-3", 1));

        assertThat(outcome.isRight()).isTrue();
        assertThat(eventStore.readStream("c-3", -1)).hasSize(1);
    }

    @Test
    void exhaustedRetriesRaiseConcurrencyConflict() {
        var repo = repository(100, 3);
        eventStore.failNextAppends = 3;

        assertThatExceptionOfType(ConcurrencyConflict.class)
                .isThrownBy(() -> repo.execute("c-4", new Increment("c-4", 1)));
    }

    @Test
    void snapshotWrittenWhenThresholdCrossedAndUsedOnLoad() {
        var repo = repository(3, 3);
        repo.execute("c-5", new Increment("c-5", 1));
        repo.execute("c-5", new Increment("c-5", 1));
        repo.execute("c-5", new Increment("c-5", 1));  // sequences 0,1,2 -> crosses threshold 3

        assertThat(snapshotStore.snapshots).containsKey("c-5");
        assertThat(snapshotStore.snapshots.get("c-5").sequenceNr()).isEqualTo(2);

        repo.execute("c-5", new Decrement("c-5", 3));
        // load happened from the snapshot: the stream read started after sequence 2
        assertThat(eventStore.lastReadAfter).isEqualTo(2);
        assertThat(snapshotStore.snapshots.get("c-5").sequenceNr()).isEqualTo(2);
    }

    @Test
    void staleSnapshotRevisionIsIgnored() {
        var repo = repository(100, 3);
        repo.execute("c-6", new Increment("c-6", 4));
        snapshotStore.save(new Snapshot("c-6", 0, 99, "999"));  // wrong revision, wrong value

        Either<DomainError, CommittedEvents> outcome = repo.execute("c-6", new Decrement("c-6", 4));

        assertThat(outcome.isRight()).isTrue();  // replay from zero: value was 4, not 999
    }
}
