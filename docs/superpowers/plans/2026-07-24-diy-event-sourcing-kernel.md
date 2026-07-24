# DIY Event-Sourcing Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the hand-rolled event-sourcing/CQRS kernel specified in
[`docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md`](../specs/2026-07-24-diy-event-sourcing-design.md),
replacing the superseded Axon Framework 5 decision (walking-skeleton Task 5).

**Architecture:** A pure-Java kernel package `com.example.banking.eventsourcing` (ports + logic,
Vavr only) with JDBC adapters in `adapter.out.eventstore`: MySQL event store with gap-free global
ordering and optimistic locking, striped async command bus, token-based tracking processors,
snapshotting, saga manager with deadlines, and given/when/then test fixtures. Kafka/Redis/domain
usage of the kernel comes in later milestones — this milestone delivers the kernel itself, its
wiring, its architecture rules, and the documentation swap.

**Tech Stack:** Java (as pinned in `pom.xml` — do not change `java.version`), Spring Boot 4.1,
Vavr, Jackson (via Spring), Spring JDBC (`JdbcTemplate` — no JPA entities for kernel tables),
Flyway, JUnit 5, AssertJ, Testcontainers (MySQL), ArchUnit.

## Preconditions

- Walking-skeleton tasks 1–7 are merged to `main` (this plan starts **after Task 7 lands**).
  Branch from `main`.
- Docker/Podman daemon running (Testcontainers).
- Design-spec PR #8 (`docs/diy-event-sourcing-design`) merged or rebased in so the spec file is
  present.

## Global Constraints

- Base package `com.example.banking`. Kernel package `com.example.banking.eventsourcing`.
- **Kernel purity:** `eventsourcing` may import only itself, `io.vavr..`, and `java..`. No Spring,
  no Jackson, no JPA anywhere in it. Enforced by ArchUnit in Task 11.
- **No reflection, no annotations** for dispatch — explicit registration everywhere.
- **Business outcomes are `Either<DomainError, …>`** (Vavr). Tests assert **both arms**.
- **Maven output convention (required):** every Maven run redirects to a file:
  `mvn -B <goals> > target/mvn-out.txt 2>&1; tail -n 60 target/mvn-out.txt`
- **Do not modify `<java.version>`** — it is owned by the walking skeleton (Java-26-vs-25 fallback
  is recorded there).
- Flyway migrations continue from `V1__baseline.sql`; this plan adds `V2`–`V6`. If an intervening
  merge already used a number, shift up and note it in the commit body.
- Conventional Commits, **no tool-attribution footer**. One task = one commit (SDD workflow: one
  PR per task, human merge gate).
- Community versions shown (`vavr 0.10.6`) follow the repo's bleeding-edge rule: resolve the
  current version at build time if newer; record any deviation in the commit body.

## File Structure (end state of this milestone)

```
src/main/java/com/example/banking/
├── eventsourcing/
│   ├── AggregateBehaviour.java        # S initial(); S evolve(S,E); Either<DomainError,List<E>> decide(S,C)
│   ├── DomainError.java               # marker: String code(); String message()
│   ├── ConcurrencyConflict.java       # RuntimeException, optimistic-lock exhaustion
│   ├── SerializedEvent.java           # record: eventId, eventType, revision, payload, metadata, occurredAt
│   ├── StoredEvent.java               # record: globalPosition, aggregateType, aggregateId, sequenceNr, event
│   ├── EventStore.java                # port: append / readStream / readAllAfter
│   ├── EventSerializer.java           # port: Object <-> SerializedEvent
│   ├── PayloadCodec.java              # port: Object <-> JSON string (snapshots, saga state, deadlines)
│   ├── EventTypeRegistry.java         # logical name + revision <-> class (explicit registration)
│   ├── Snapshot.java                  # record: aggregateId, sequenceNr, revision, payload
│   ├── SnapshotStore.java             # port: load / save
│   ├── EventSourcingRepository.java   # load + decide + append + retry + snapshot threshold
│   ├── CommittedEvents.java           # record: aggregateId, lastSequenceNr, events
│   ├── CommandHandler.java            # Either<DomainError,CommittedEvents> handle(C)
│   ├── CommandBus.java                # port: dispatch -> CompletableFuture, register
│   ├── StripedCommandBus.java         # hash(aggregateId) % N single-thread stripes
│   ├── TokenStore.java                # port: load / save / reset
│   ├── TransactionalRunner.java       # port: void inTransaction(Runnable)
│   ├── EventHandler.java              # void handle(StoredEvent)
│   ├── TrackingProcessor.java         # poll loop; processOnce(); start/stop
│   ├── SagaBehaviour.java             # associationKey, startsSaga, initial, react
│   ├── SagaUpdate.java                # record: state, terminal, commands, schedule, cancel
│   ├── DeadlineRequest.java           # record: deadlineId, after, payload
│   ├── SagaInstance.java              # record: sagaId, sagaType, statePayload, terminal
│   ├── SagaStore.java                 # port: findByAssociation / findById / insert / save
│   ├── DeadlineScheduler.java         # port: schedule / cancel
│   └── SagaManager.java               # EventHandler; correlate -> react -> persist -> dispatch
└── adapter/out/eventstore/
    ├── JdbcEventStore.java            # gap-free global_position, uq_stream optimistic locking
    ├── JacksonPayloadCodec.java
    ├── Upcaster.java                  # (eventType, fromRevision, JsonNode -> JsonNode)
    ├── UpcasterChain.java
    ├── JacksonEventSerializer.java
    ├── JdbcSnapshotStore.java
    ├── JdbcTokenStore.java
    ├── SpringTransactionalRunner.java
    ├── JdbcSagaStore.java
    ├── JdbcDeadlineScheduler.java
    ├── DeadlinePoller.java            # polls due deadlines -> SagaManager
    ├── EventSourcingProperties.java   # banking.eventsourcing.* config record
    └── EventSourcingConfig.java       # bean wiring

src/main/resources/db/migration/
├── V2__event_store.sql
├── V3__snapshot.sql
├── V4__tracking_token.sql
├── V5__saga.sql
└── V6__deadline.sql

src/test/java/com/example/banking/eventsourcing/
├── fixture/AggregateTestFixture.java  # given().when().expectEvents()/expectError()
├── fixture/SagaTestFixture.java
├── support/CounterBehaviour.java      # toy aggregate for kernel tests
├── support/InMemoryEventStore.java
├── support/InMemorySnapshotStore.java
└── ... unit tests

src/test/java/com/example/banking/infra/           # integration tests (Testcontainers)
src/test/java/com/example/banking/architecture/    # EventSourcingKernelRulesTest
```

## Task overview

| # | Task | Deliverable |
| --- | --- | --- |
| 1 | Vavr + kernel core types + AggregateTestFixture | pure GWT fixture proven on toy aggregate |
| 2 | EventStore port + JDBC impl + V2 migration | gap-free ordered, optimistically locked store |
| 3 | Serialization: codec, registry, upcasters | JSON round-trip + revision upcast |
| 4 | SnapshotStore + JDBC impl + V3 migration | latest-only snapshot persistence |
| 5 | EventSourcingRepository | load/decide/append/retry/snapshot threshold |
| 6 | CommandBus (striped, async) | per-aggregate ordering, Either results |
| 7 | TokenStore + TrackingProcessor + V4 migration | resumable exactly-once projections, rebuild |
| 8 | SagaStore + SagaManager + SagaTestFixture + V5 | correlated saga state machine runtime |
| 9 | DeadlineScheduler + poller + V6 migration | saga timeouts with cancel-on-terminal |
| 10 | Spring wiring + properties + boot test | kernel beans in the app context |
| 11 | ArchUnit kernel/domain purity rules | dependency rules locked |
| 12 | Documentation swap (ARCHITECTURE/CLAUDE/plan) | no stale Axon references |

---

### Task 1: Vavr dependency, kernel core types, AggregateTestFixture

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/example/banking/eventsourcing/DomainError.java`
- Create: `src/main/java/com/example/banking/eventsourcing/AggregateBehaviour.java`
- Create: `src/test/java/com/example/banking/eventsourcing/fixture/AggregateTestFixture.java`
- Create: `src/test/java/com/example/banking/eventsourcing/support/CounterBehaviour.java`
- Test: `src/test/java/com/example/banking/eventsourcing/AggregateTestFixtureTest.java`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `AggregateBehaviour<S,C,E>` (`String aggregateType(); S initial(); S evolve(S state, E event); io.vavr.control.Either<DomainError, java.util.List<E>> decide(S state, C command)`), `DomainError` (`String code(); String message()`), `AggregateTestFixture.forBehaviour(b).given(events...).when(cmd).expectEvents(...)/.expectError(...)`, and the test-scope `CounterBehaviour` toy aggregate reused by Tasks 5–7.

- [ ] **Step 1: Add Vavr to `pom.xml`**

After the `spring-boot-starter-kafka` dependency block, add:

```xml
<dependency>
    <groupId>io.vavr</groupId>
    <artifactId>vavr</artifactId>
    <version>0.10.6</version>
</dependency>
```

(Resolve a newer release from Maven Central if one exists; record the chosen version in the commit body.)

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/eventsourcing/AggregateTestFixtureTest.java`:

```java
package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.fixture.AggregateTestFixture;
import com.example.banking.eventsourcing.support.CounterBehaviour;
import org.junit.jupiter.api.Test;

import static com.example.banking.eventsourcing.support.CounterBehaviour.*;

class AggregateTestFixtureTest {

    private final AggregateTestFixture<Counter, CounterCommand, CounterEvent> fixture =
            AggregateTestFixture.forBehaviour(new CounterBehaviour());

    @Test
    void rightArm_incrementEmitsIncremented() {
        fixture.given(new Incremented(2))
                .when(new Increment("c-1", 3))
                .expectEvents(new Incremented(3));
    }

    @Test
    void leftArm_decrementBelowZeroIsRejected() {
        fixture.given(new Incremented(1))
                .when(new Decrement("c-1", 5))
                .expectError(new NegativeCounter(1, 5));
    }
}
```

`src/test/java/com/example/banking/eventsourcing/support/CounterBehaviour.java`:

```java
package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.AggregateBehaviour;
import com.example.banking.eventsourcing.DomainError;
import io.vavr.control.Either;

import java.util.List;

/** Toy aggregate used by kernel tests only: a counter that must never go below zero. */
public final class CounterBehaviour
        implements AggregateBehaviour<CounterBehaviour.Counter, CounterBehaviour.CounterCommand, CounterBehaviour.CounterEvent> {

    public sealed interface CounterCommand permits Increment, Decrement {
        String counterId();
    }
    public record Increment(String counterId, int by) implements CounterCommand {}
    public record Decrement(String counterId, int by) implements CounterCommand {}

    public sealed interface CounterEvent permits Incremented, Decremented {}
    public record Incremented(int by) implements CounterEvent {}
    public record Decremented(int by) implements CounterEvent {}

    public record Counter(int value) {}

    public record NegativeCounter(int value, int attempted) implements DomainError {
        @Override public String code() { return "counter.negative"; }
        @Override public String message() { return "cannot decrement " + value + " by " + attempted; }
    }

    @Override public String aggregateType() { return "Counter"; }

    @Override public Counter initial() { return new Counter(0); }

    @Override public Counter evolve(Counter state, CounterEvent event) {
        return switch (event) {
            case Incremented e -> new Counter(state.value() + e.by());
            case Decremented e -> new Counter(state.value() - e.by());
        };
    }

    @Override public Either<DomainError, List<CounterEvent>> decide(Counter state, CounterCommand command) {
        return switch (command) {
            case Increment c -> Either.right(List.of(new Incremented(c.by())));
            case Decrement c -> state.value() - c.by() < 0
                    ? Either.left(new NegativeCounter(state.value(), c.by()))
                    : Either.right(List.of(new Decremented(c.by())));
        };
    }
}
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=AggregateTestFixtureTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR — `AggregateBehaviour`, `DomainError`, `AggregateTestFixture` do not exist.

- [ ] **Step 4: Implement the kernel types and the fixture**

`src/main/java/com/example/banking/eventsourcing/DomainError.java`:

```java
package com.example.banking.eventsourcing;

/** A business-rule failure. Never thrown — always carried in the left arm of an Either. */
public interface DomainError {
    String code();
    String message();
}
```

`src/main/java/com/example/banking/eventsourcing/AggregateBehaviour.java`:

```java
package com.example.banking.eventsourcing;

import io.vavr.control.Either;

import java.util.List;

/**
 * The functional aggregate contract: pure decide/evolve, no annotations, no reflection.
 *
 * @param <S> aggregate state (immutable)
 * @param <C> command supertype
 * @param <E> event supertype
 */
public interface AggregateBehaviour<S, C, E> {

    /** Logical stream-type name stored with every event (e.g. "Account"). */
    String aggregateType();

    S initial();

    S evolve(S state, E event);

    Either<DomainError, List<E>> decide(S state, C command);
}
```

`src/test/java/com/example/banking/eventsourcing/fixture/AggregateTestFixture.java`:

```java
package com.example.banking.eventsourcing.fixture;

import com.example.banking.eventsourcing.AggregateBehaviour;
import com.example.banking.eventsourcing.DomainError;
import io.vavr.control.Either;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure given/when/then fixture: folds given events with evolve, runs decide, asserts either arm. */
public final class AggregateTestFixture<S, C, E> {

    private final AggregateBehaviour<S, C, E> behaviour;
    private S state;

    private AggregateTestFixture(AggregateBehaviour<S, C, E> behaviour) {
        this.behaviour = behaviour;
        this.state = behaviour.initial();
    }

    public static <S, C, E> AggregateTestFixture<S, C, E> forBehaviour(AggregateBehaviour<S, C, E> behaviour) {
        return new AggregateTestFixture<>(behaviour);
    }

    @SafeVarargs
    public final AggregateTestFixture<S, C, E> given(E... events) {
        for (E event : events) {
            state = behaviour.evolve(state, event);
        }
        return this;
    }

    public When when(C command) {
        return new When(behaviour.decide(state, command));
    }

    public final class When {
        private final Either<DomainError, List<E>> outcome;

        private When(Either<DomainError, List<E>> outcome) {
            this.outcome = outcome;
        }

        @SafeVarargs
        public final void expectEvents(E... expected) {
            assertThat(outcome.isRight())
                    .as("expected events %s but got error %s", List.of(expected), outcome.swap().getOrNull())
                    .isTrue();
            assertThat(outcome.get()).containsExactly(expected);
        }

        public void expectError(DomainError expected) {
            assertThat(outcome.isLeft())
                    .as("expected error %s but got events %s", expected, outcome.getOrNull())
                    .isTrue();
            assertThat(outcome.getLeft()).isEqualTo(expected);
        }
    }
}
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=AggregateTestFixtureTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/java/com/example/banking/eventsourcing src/test/java/com/example/banking/eventsourcing
git commit -m "feat: add event-sourcing kernel core types and GWT test fixture"
```

---

### Task 2: EventStore port, JDBC implementation, V2 migration

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/SerializedEvent.java`
- Create: `src/main/java/com/example/banking/eventsourcing/StoredEvent.java`
- Create: `src/main/java/com/example/banking/eventsourcing/ConcurrencyConflict.java`
- Create: `src/main/java/com/example/banking/eventsourcing/EventStore.java`
- Create: `src/main/java/com/example/banking/eventsourcing/TransactionalRunner.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/SpringTransactionalRunner.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JdbcEventStore.java`
- Create: `src/main/resources/db/migration/V2__event_store.sql`
- Test: `src/test/java/com/example/banking/infra/JdbcEventStoreTest.java`

**Interfaces:**
- Consumes: nothing from Task 1 (parallel-safe).
- Produces:
  - `record SerializedEvent(String eventId, String eventType, int revision, String payload, String metadata, java.time.Instant occurredAt)`
  - `record StoredEvent(long globalPosition, String aggregateType, String aggregateId, long sequenceNr, SerializedEvent event)`
  - `interface EventStore { void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events); List<StoredEvent> readStream(String aggregateId, long afterSequenceNr); List<StoredEvent> readAllAfter(long globalPosition, int limit); }` — `append` throws `ConcurrencyConflict` (unchecked) on a stale `expectedVersion`; a new stream has `expectedVersion = -1` (sequences are 0-based).
  - `interface TransactionalRunner { void inTransaction(Runnable work); }`
  - `class SpringTransactionalRunner implements TransactionalRunner` (constructor: `TransactionTemplate`)
  - `class JdbcEventStore implements EventStore` (constructor: `JdbcTemplate`, `TransactionalRunner`)

- [ ] **Step 1: Write the migration**

`src/main/resources/db/migration/V2__event_store.sql`:

```sql
-- Event store: source of truth. global_position is assigned from event_store_sequence
-- under FOR UPDATE in the same transaction as the insert, so commit order == position
-- order and rollbacks leave no gaps (see design spec §4).
CREATE TABLE event_store_sequence (
    id            TINYINT NOT NULL PRIMARY KEY,
    next_position BIGINT  NOT NULL
);
INSERT INTO event_store_sequence (id, next_position) VALUES (1, 1);

CREATE TABLE domain_event (
    global_position BIGINT       NOT NULL PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    CHAR(36)     NOT NULL,
    sequence_nr     BIGINT       NOT NULL,
    event_id        CHAR(36)     NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    revision        INT          NOT NULL,
    payload         JSON         NOT NULL,
    metadata        JSON         NOT NULL,
    occurred_at     TIMESTAMP(6) NOT NULL,
    UNIQUE KEY uq_stream (aggregate_id, sequence_nr)
);
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/infra/JdbcEventStoreTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.JdbcEventStore;
import com.example.banking.adapter.out.eventstore.SpringTransactionalRunner;
import com.example.banking.eventsourcing.ConcurrencyConflict;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest
@Import(ContainersConfig.class)
class JdbcEventStoreTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager txManager;

    EventStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcEventStore(jdbc, new SpringTransactionalRunner(new TransactionTemplate(txManager)));
        jdbc.update("DELETE FROM domain_event");
        jdbc.update("UPDATE event_store_sequence SET next_position = 1");
    }

    private static SerializedEvent event(String type) {
        return new SerializedEvent(UUID.randomUUID().toString(), type, 1, "{\"by\":1}", "{}", Instant.now());
    }

    @Test
    void appendsAndReadsBackAStreamInOrder() {
        store.append("Counter", "c-1", -1, List.of(event("Incremented"), event("Incremented")));
        store.append("Counter", "c-1", 1, List.of(event("Decremented")));

        List<StoredEvent> stream = store.readStream("c-1", -1);

        assertThat(stream).hasSize(3);
        assertThat(stream).extracting(StoredEvent::sequenceNr).containsExactly(0L, 1L, 2L);
        assertThat(stream.get(2).event().eventType()).isEqualTo("Decremented");
        assertThat(store.readStream("c-1", 1)).hasSize(1);
    }

    @Test
    void staleExpectedVersionRaisesConcurrencyConflict() {
        store.append("Counter", "c-2", -1, List.of(event("Incremented")));

        assertThatExceptionOfType(ConcurrencyConflict.class)
                .isThrownBy(() -> store.append("Counter", "c-2", -1, List.of(event("Incremented"))));
    }

    @Test
    void globalPositionsAreGapFreeAndOrderedAcrossStreams() {
        store.append("Counter", "c-3", -1, List.of(event("Incremented")));
        store.append("Counter", "c-4", -1, List.of(event("Incremented"), event("Incremented")));

        List<StoredEvent> all = store.readAllAfter(0, 10);

        assertThat(all).extracting(StoredEvent::globalPosition).containsExactly(1L, 2L, 3L);
        assertThat(store.readAllAfter(1, 10)).hasSize(2);
        assertThat(store.readAllAfter(3, 10)).isEmpty();
    }

    @Test
    void failedAppendLeavesNoGap() {
        store.append("Counter", "c-5", -1, List.of(event("Incremented")));
        assertThatExceptionOfType(ConcurrencyConflict.class)
                .isThrownBy(() -> store.append("Counter", "c-5", -1, List.of(event("Incremented"))));
        store.append("Counter", "c-6", -1, List.of(event("Incremented")));

        assertThat(store.readAllAfter(0, 10))
                .extracting(StoredEvent::globalPosition).containsExactly(1L, 2L);
    }
}
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=JdbcEventStoreTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR — kernel types and `JdbcEventStore` do not exist.

- [ ] **Step 4: Implement kernel types**

`src/main/java/com/example/banking/eventsourcing/SerializedEvent.java`:

```java
package com.example.banking.eventsourcing;

import java.time.Instant;

/** A domain event in storage form: JSON payload plus type/revision for upcasting. */
public record SerializedEvent(
        String eventId,
        String eventType,
        int revision,
        String payload,
        String metadata,
        Instant occurredAt) {
}
```

`src/main/java/com/example/banking/eventsourcing/StoredEvent.java`:

```java
package com.example.banking.eventsourcing;

/** A committed event with its position in the store. */
public record StoredEvent(
        long globalPosition,
        String aggregateType,
        String aggregateId,
        long sequenceNr,
        SerializedEvent event) {
}
```

`src/main/java/com/example/banking/eventsourcing/ConcurrencyConflict.java`:

```java
package com.example.banking.eventsourcing;

/** Optimistic-locking failure: another command appended to the stream first. */
public final class ConcurrencyConflict extends RuntimeException {

    public ConcurrencyConflict(String aggregateId, long expectedVersion) {
        super("concurrent append to aggregate " + aggregateId + " at expected version " + expectedVersion);
    }
}
```

`src/main/java/com/example/banking/eventsourcing/EventStore.java`:

```java
package com.example.banking.eventsourcing;

import java.util.List;

/**
 * Append-only event store. Sequences are 0-based per stream; a new stream is appended with
 * {@code expectedVersion = -1}. Global positions are strictly increasing, gap-free, and follow
 * commit order.
 */
public interface EventStore {

    /** @throws ConcurrencyConflict when expectedVersion is stale */
    void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events);

    List<StoredEvent> readStream(String aggregateId, long afterSequenceNr);

    List<StoredEvent> readAllAfter(long globalPosition, int limit);
}
```

`src/main/java/com/example/banking/eventsourcing/TransactionalRunner.java`:

```java
package com.example.banking.eventsourcing;

/** Port over the platform transaction manager so the kernel stays Spring-free. */
public interface TransactionalRunner {
    void inTransaction(Runnable work);
}
```

- [ ] **Step 5: Implement the JDBC adapter**

`src/main/java/com/example/banking/adapter/out/eventstore/SpringTransactionalRunner.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.TransactionalRunner;
import org.springframework.transaction.support.TransactionTemplate;

public final class SpringTransactionalRunner implements TransactionalRunner {

    private final TransactionTemplate transactionTemplate;

    public SpringTransactionalRunner(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public void inTransaction(Runnable work) {
        transactionTemplate.executeWithoutResult(status -> work.run());
    }
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/JdbcEventStore.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.ConcurrencyConflict;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.util.List;

public final class JdbcEventStore implements EventStore {

    private static final RowMapper<StoredEvent> ROW_MAPPER = (rs, rowNum) -> new StoredEvent(
            rs.getLong("global_position"),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getLong("sequence_nr"),
            new SerializedEvent(
                    rs.getString("event_id"),
                    rs.getString("event_type"),
                    rs.getInt("revision"),
                    rs.getString("payload"),
                    rs.getString("metadata"),
                    rs.getTimestamp("occurred_at").toInstant()));

    private final JdbcTemplate jdbc;
    private final TransactionalRunner tx;

    public JdbcEventStore(JdbcTemplate jdbc, TransactionalRunner tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @Override
    public void append(String aggregateType, String aggregateId, long expectedVersion, List<SerializedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        tx.inTransaction(() -> {
            long firstPosition = jdbc.queryForObject(
                    "SELECT next_position FROM event_store_sequence WHERE id = 1 FOR UPDATE", Long.class);
            try {
                for (int i = 0; i < events.size(); i++) {
                    SerializedEvent e = events.get(i);
                    jdbc.update("""
                                    INSERT INTO domain_event (global_position, aggregate_type, aggregate_id,
                                        sequence_nr, event_id, event_type, revision, payload, metadata, occurred_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            firstPosition + i, aggregateType, aggregateId, expectedVersion + 1 + i,
                            e.eventId(), e.eventType(), e.revision(), e.payload(), e.metadata(),
                            Timestamp.from(e.occurredAt()));
                }
            } catch (DuplicateKeyException conflict) {
                throw new ConcurrencyConflict(aggregateId, expectedVersion);
            }
            jdbc.update("UPDATE event_store_sequence SET next_position = ? WHERE id = 1",
                    firstPosition + events.size());
        });
    }

    @Override
    public List<StoredEvent> readStream(String aggregateId, long afterSequenceNr) {
        return jdbc.query("""
                        SELECT * FROM domain_event
                        WHERE aggregate_id = ? AND sequence_nr > ?
                        ORDER BY sequence_nr""",
                ROW_MAPPER, aggregateId, afterSequenceNr);
    }

    @Override
    public List<StoredEvent> readAllAfter(long globalPosition, int limit) {
        return jdbc.query("""
                        SELECT * FROM domain_event
                        WHERE global_position > ?
                        ORDER BY global_position
                        LIMIT ?""",
                ROW_MAPPER, globalPosition, limit);
    }
}
```

- [ ] **Step 6: Run the test — verify it passes**

Run: `mvn -B test -Dtest=JdbcEventStoreTest > target/mvn-out.txt 2>&1; tail -n 40 target/mvn-out.txt`
Expected: PASS (4 tests). Flyway applies `V2__event_store.sql` against the Testcontainers MySQL.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/main/java/com/example/banking/adapter src/main/resources/db/migration/V2__event_store.sql src/test/java/com/example/banking/infra/JdbcEventStoreTest.java
git commit -m "feat: add MySQL event store with gap-free ordering and optimistic locking"
```

---

### Task 3: Serialization — PayloadCodec, EventTypeRegistry, EventSerializer, upcasters

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/PayloadCodec.java`
- Create: `src/main/java/com/example/banking/eventsourcing/EventTypeRegistry.java`
- Create: `src/main/java/com/example/banking/eventsourcing/EventSerializer.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JacksonPayloadCodec.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/Upcaster.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/UpcasterChain.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JacksonEventSerializer.java`
- Test: `src/test/java/com/example/banking/adapter/out/eventstore/JacksonEventSerializerTest.java`

**Interfaces:**
- Consumes: `SerializedEvent` (Task 2).
- Produces:
  - `interface PayloadCodec { String encode(Object value); <T> T decode(String json, Class<T> type); }`
  - `class EventTypeRegistry { void register(String name, int currentRevision, Class<?> type); EventType byClass(Class<?> type); EventType byName(String name); record EventType(String name, int currentRevision, Class<?> type) {} }` — throws `IllegalArgumentException` for unregistered types.
  - `interface EventSerializer { SerializedEvent serialize(Object event, Map<String,String> metadata); Object deserialize(SerializedEvent event); }`
  - `interface Upcaster { String eventType(); int fromRevision(); JsonNode upcast(JsonNode payload); }`
  - `class JacksonEventSerializer implements EventSerializer` (constructor: `ObjectMapper`, `EventTypeRegistry`, `UpcasterChain`, `java.time.Clock`)

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/adapter/out/eventstore/JacksonEventSerializerTest.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.SerializedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class JacksonEventSerializerTest {

    record MoneyDepositedV2(String accountId, long amountCents, String source) {}

    private final ObjectMapper mapper = new ObjectMapper();
    private final Clock clock = Clock.fixed(Instant.parse("2026-07-24T10:00:00Z"), ZoneOffset.UTC);

    private EventTypeRegistry registry() {
        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("MoneyDeposited", 2, MoneyDepositedV2.class);
        return registry;
    }

    @Test
    void roundTripsAnEventAtCurrentRevision() {
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of()), clock);
        MoneyDepositedV2 event = new MoneyDepositedV2("a-1", 500, "cash");

        SerializedEvent serialized = serializer.serialize(event, Map.of("userId", "u-1"));

        assertThat(serialized.eventType()).isEqualTo("MoneyDeposited");
        assertThat(serialized.revision()).isEqualTo(2);
        assertThat(serialized.occurredAt()).isEqualTo(Instant.parse("2026-07-24T10:00:00Z"));
        assertThat(serialized.metadata()).contains("\"userId\":\"u-1\"");
        assertThat(serializer.deserialize(serialized)).isEqualTo(event);
    }

    @Test
    void upcastsAnOldRevisionOnRead() {
        Upcaster v1ToV2 = new Upcaster() {
            @Override public String eventType() { return "MoneyDeposited"; }
            @Override public int fromRevision() { return 1; }
            @Override public com.fasterxml.jackson.databind.JsonNode upcast(com.fasterxml.jackson.databind.JsonNode payload) {
                ((ObjectNode) payload).put("source", "unknown");
                return payload;
            }
        };
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of(v1ToV2)), clock);
        SerializedEvent v1 = new SerializedEvent("e-1", "MoneyDeposited", 1,
                "{\"accountId\":\"a-1\",\"amountCents\":500}", "{}", Instant.parse("2026-01-01T00:00:00Z"));

        Object event = serializer.deserialize(v1);

        assertThat(event).isEqualTo(new MoneyDepositedV2("a-1", 500, "unknown"));
    }

    @Test
    void unregisteredTypeIsRejectedOnBothArms() {
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of()), clock);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> serializer.serialize("not registered", Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> serializer.deserialize(new SerializedEvent(
                        "e-2", "Unknown", 1, "{}", "{}", Instant.now())));
    }
}
```

- [ ] **Step 2: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=JacksonEventSerializerTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 3: Implement kernel ports**

`src/main/java/com/example/banking/eventsourcing/PayloadCodec.java`:

```java
package com.example.banking.eventsourcing;

/** JSON codec port used for snapshots, saga state, and deadline payloads. */
public interface PayloadCodec {
    String encode(Object value);
    <T> T decode(String json, Class<T> type);
}
```

`src/main/java/com/example/banking/eventsourcing/EventTypeRegistry.java`:

```java
package com.example.banking.eventsourcing;

import java.util.HashMap;
import java.util.Map;

/** Explicit event-type registration: logical name + current revision per event class. */
public final class EventTypeRegistry {

    public record EventType(String name, int currentRevision, Class<?> type) {}

    private final Map<Class<?>, EventType> byClass = new HashMap<>();
    private final Map<String, EventType> byName = new HashMap<>();

    public void register(String name, int currentRevision, Class<?> type) {
        EventType eventType = new EventType(name, currentRevision, type);
        byClass.put(type, eventType);
        byName.put(name, eventType);
    }

    public EventType byClass(Class<?> type) {
        EventType found = byClass.get(type);
        if (found == null) {
            throw new IllegalArgumentException("event class not registered: " + type.getName());
        }
        return found;
    }

    public EventType byName(String name) {
        EventType found = byName.get(name);
        if (found == null) {
            throw new IllegalArgumentException("event type not registered: " + name);
        }
        return found;
    }
}
```

`src/main/java/com/example/banking/eventsourcing/EventSerializer.java`:

```java
package com.example.banking.eventsourcing;

import java.util.Map;

/** Turns domain events into storage form and back, applying upcasters on read. */
public interface EventSerializer {
    SerializedEvent serialize(Object event, Map<String, String> metadata);
    Object deserialize(SerializedEvent event);
}
```

- [ ] **Step 4: Implement the Jackson adapter**

`src/main/java/com/example/banking/adapter/out/eventstore/JacksonPayloadCodec.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.PayloadCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JacksonPayloadCodec implements PayloadCodec {

    private final ObjectMapper mapper;

    public JacksonPayloadCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String encode(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot encode " + value.getClass().getName(), e);
        }
    }

    @Override
    public <T> T decode(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot decode into " + type.getName(), e);
        }
    }
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/Upcaster.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.fasterxml.jackson.databind.JsonNode;

/** Pure payload migration for one revision step of one event type. */
public interface Upcaster {
    String eventType();
    int fromRevision();
    JsonNode upcast(JsonNode payload);
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/UpcasterChain.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.fasterxml.jackson.databind.JsonNode;

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
```

`src/main/java/com/example/banking/adapter/out/eventstore/JacksonEventSerializer.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.EventSerializer;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.SerializedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

public final class JacksonEventSerializer implements EventSerializer {

    private final ObjectMapper mapper;
    private final EventTypeRegistry registry;
    private final UpcasterChain upcasters;
    private final Clock clock;

    public JacksonEventSerializer(ObjectMapper mapper, EventTypeRegistry registry,
                                  UpcasterChain upcasters, Clock clock) {
        this.mapper = mapper;
        this.registry = registry;
        this.upcasters = upcasters;
        this.clock = clock;
    }

    @Override
    public SerializedEvent serialize(Object event, Map<String, String> metadata) {
        EventTypeRegistry.EventType type = registry.byClass(event.getClass());
        try {
            return new SerializedEvent(
                    UUID.randomUUID().toString(),
                    type.name(),
                    type.currentRevision(),
                    mapper.writeValueAsString(event),
                    mapper.writeValueAsString(metadata),
                    clock.instant());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialize " + type.name(), e);
        }
    }

    @Override
    public Object deserialize(SerializedEvent event) {
        EventTypeRegistry.EventType type = registry.byName(event.eventType());
        try {
            JsonNode payload = mapper.readTree(event.payload());
            JsonNode upcast = upcasters.upcast(event.eventType(), event.revision(), type.currentRevision(), payload);
            return mapper.treeToValue(upcast, type.type());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot deserialize " + event.eventType(), e);
        }
    }
}
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=JacksonEventSerializerTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/main/java/com/example/banking/adapter/out/eventstore src/test/java/com/example/banking/adapter
git commit -m "feat: add JSON event serialization with type registry and upcaster chain"
```

---

### Task 4: SnapshotStore, JDBC implementation, V3 migration

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/Snapshot.java`
- Create: `src/main/java/com/example/banking/eventsourcing/SnapshotStore.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JdbcSnapshotStore.java`
- Create: `src/main/resources/db/migration/V3__snapshot.sql`
- Test: `src/test/java/com/example/banking/infra/JdbcSnapshotStoreTest.java`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `record Snapshot(String aggregateId, long sequenceNr, int revision, String payload)`
  - `interface SnapshotStore { java.util.Optional<Snapshot> load(String aggregateId); void save(Snapshot snapshot); }` — `save` overwrites (latest-only).
  - `class JdbcSnapshotStore implements SnapshotStore` (constructor: `JdbcTemplate`)

- [ ] **Step 1: Write the migration**

`src/main/resources/db/migration/V3__snapshot.sql`:

```sql
-- Latest-only aggregate snapshots; replay from zero remains the fallback truth.
CREATE TABLE snapshot (
    aggregate_id CHAR(36) NOT NULL PRIMARY KEY,
    sequence_nr  BIGINT   NOT NULL,
    revision     INT      NOT NULL,
    payload      JSON     NOT NULL
);
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/infra/JdbcSnapshotStoreTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.JdbcSnapshotStore;
import com.example.banking.eventsourcing.Snapshot;
import com.example.banking.eventsourcing.SnapshotStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class JdbcSnapshotStoreTest {

    @Autowired JdbcTemplate jdbc;

    SnapshotStore store;

    @BeforeEach
    void setUp() {
        store = new JdbcSnapshotStore(jdbc);
        jdbc.update("DELETE FROM snapshot");
    }

    @Test
    void missingSnapshotIsEmpty() {
        assertThat(store.load("a-none")).isEmpty();
    }

    @Test
    void savesAndOverwritesLatestOnly() {
        store.save(new Snapshot("a-1", 99, 1, "{\"value\":10}"));
        store.save(new Snapshot("a-1", 199, 1, "{\"value\":20}"));

        assertThat(store.load("a-1")).hasValueSatisfying(s -> {
            assertThat(s.sequenceNr()).isEqualTo(199);
            assertThat(s.payload()).contains("20");
        });
    }
}
```

Note: MySQL normalizes JSON column formatting, so the payload assertion checks semantic content,
not the exact string.

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=JdbcSnapshotStoreTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 4: Implement**

`src/main/java/com/example/banking/eventsourcing/Snapshot.java`:

```java
package com.example.banking.eventsourcing;

/** Latest-only aggregate snapshot. revision guards against stale state shapes. */
public record Snapshot(String aggregateId, long sequenceNr, int revision, String payload) {
}
```

`src/main/java/com/example/banking/eventsourcing/SnapshotStore.java`:

```java
package com.example.banking.eventsourcing;

import java.util.Optional;

public interface SnapshotStore {
    Optional<Snapshot> load(String aggregateId);
    void save(Snapshot snapshot);
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/JdbcSnapshotStore.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.Snapshot;
import com.example.banking.eventsourcing.SnapshotStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

public final class JdbcSnapshotStore implements SnapshotStore {

    private final JdbcTemplate jdbc;

    public JdbcSnapshotStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Snapshot> load(String aggregateId) {
        return jdbc.query("""
                        SELECT aggregate_id, sequence_nr, revision, payload
                        FROM snapshot WHERE aggregate_id = ?""",
                (rs, rowNum) -> new Snapshot(
                        rs.getString("aggregate_id"),
                        rs.getLong("sequence_nr"),
                        rs.getInt("revision"),
                        rs.getString("payload")),
                aggregateId).stream().findFirst();
    }

    @Override
    public void save(Snapshot snapshot) {
        jdbc.update("""
                        INSERT INTO snapshot (aggregate_id, sequence_nr, revision, payload)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE sequence_nr = VALUES(sequence_nr),
                            revision = VALUES(revision), payload = VALUES(payload)""",
                snapshot.aggregateId(), snapshot.sequenceNr(), snapshot.revision(), snapshot.payload());
    }
}
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=JdbcSnapshotStoreTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/main/java/com/example/banking/adapter/out/eventstore src/main/resources/db/migration/V3__snapshot.sql src/test/java/com/example/banking/infra/JdbcSnapshotStoreTest.java
git commit -m "feat: add latest-only snapshot store on MySQL"
```

---

### Task 5: EventSourcingRepository — load, decide, append, retry, snapshot threshold

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/CommittedEvents.java`
- Create: `src/main/java/com/example/banking/eventsourcing/EventSourcingRepository.java`
- Create: `src/test/java/com/example/banking/eventsourcing/support/InMemoryEventStore.java`
- Create: `src/test/java/com/example/banking/eventsourcing/support/InMemorySnapshotStore.java`
- Test: `src/test/java/com/example/banking/eventsourcing/EventSourcingRepositoryTest.java`

**Interfaces:**
- Consumes: `AggregateBehaviour`, `DomainError` (Task 1); `EventStore`, `SerializedEvent`, `StoredEvent`, `ConcurrencyConflict` (Task 2); `EventSerializer`, `PayloadCodec` (Task 3); `SnapshotStore`, `Snapshot` (Task 4).
- Produces:
  - `record CommittedEvents(String aggregateId, long lastSequenceNr, java.util.List<Object> events)`
  - `class EventSourcingRepository<S, C, E>` — constructor `(AggregateBehaviour<S,C,E> behaviour, EventStore eventStore, SnapshotStore snapshotStore, EventSerializer eventSerializer, PayloadCodec codec, Class<S> stateType, int stateRevision, int snapshotThreshold, int maxAttempts)`; method `Either<DomainError, CommittedEvents> execute(String aggregateId, C command)` — throws `ConcurrencyConflict` only after `maxAttempts` exhausted (the caller maps it to HTTP 409).
  - Test-scope `InMemoryEventStore` / `InMemorySnapshotStore` fakes (reused by Task 6+ unit tests).

- [ ] **Step 1: Write the in-memory fakes**

`src/test/java/com/example/banking/eventsourcing/support/InMemoryEventStore.java`:

```java
package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.ConcurrencyConflict;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;

import java.util.ArrayList;
import java.util.List;

/** Test fake. Set failNextAppends > 0 to simulate optimistic-lock conflicts. */
public final class InMemoryEventStore implements EventStore {

    private final List<StoredEvent> events = new ArrayList<>();
    public int failNextAppends = 0;
    public long lastReadAfter = Long.MIN_VALUE;

    @Override
    public synchronized void append(String aggregateType, String aggregateId,
                                    long expectedVersion, List<SerializedEvent> newEvents) {
        if (failNextAppends > 0) {
            failNextAppends--;
            throw new ConcurrencyConflict(aggregateId, expectedVersion);
        }
        long current = events.stream()
                .filter(e -> e.aggregateId().equals(aggregateId))
                .mapToLong(StoredEvent::sequenceNr).max().orElse(-1);
        if (current != expectedVersion) {
            throw new ConcurrencyConflict(aggregateId, expectedVersion);
        }
        for (int i = 0; i < newEvents.size(); i++) {
            events.add(new StoredEvent(events.size() + 1L, aggregateType, aggregateId,
                    expectedVersion + 1 + i, newEvents.get(i)));
        }
    }

    @Override
    public synchronized List<StoredEvent> readStream(String aggregateId, long afterSequenceNr) {
        lastReadAfter = afterSequenceNr;
        return events.stream()
                .filter(e -> e.aggregateId().equals(aggregateId) && e.sequenceNr() > afterSequenceNr)
                .toList();
    }

    @Override
    public synchronized List<StoredEvent> readAllAfter(long globalPosition, int limit) {
        return events.stream()
                .filter(e -> e.globalPosition() > globalPosition)
                .limit(limit)
                .toList();
    }
}
```

`src/test/java/com/example/banking/eventsourcing/support/InMemorySnapshotStore.java`:

```java
package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.Snapshot;
import com.example.banking.eventsourcing.SnapshotStore;

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
```

The tests also need a trivial serializer pair for the toy `CounterBehaviour`. Add to
`src/test/java/com/example/banking/eventsourcing/support/CounterBehaviour.java` (same file, bottom
of the class):

```java
    /** Test serializer: encodes counter events as "<SimpleName>:<by>". */
    public static com.example.banking.eventsourcing.EventSerializer testSerializer() {
        return new com.example.banking.eventsourcing.EventSerializer() {
            @Override
            public com.example.banking.eventsourcing.SerializedEvent serialize(Object event, java.util.Map<String, String> metadata) {
                int by = event instanceof Incremented i ? i.by() : ((Decremented) event).by();
                return new com.example.banking.eventsourcing.SerializedEvent(
                        java.util.UUID.randomUUID().toString(),
                        event.getClass().getSimpleName(), 1,
                        event.getClass().getSimpleName() + ":" + by, "{}",
                        java.time.Instant.EPOCH);
            }

            @Override
            public Object deserialize(com.example.banking.eventsourcing.SerializedEvent event) {
                int by = Integer.parseInt(event.payload().split(":")[1]);
                return event.payload().startsWith("Incremented") ? new Incremented(by) : new Decremented(by);
            }
        };
    }

    /** Test codec for Counter state: encodes the int value as a string. */
    public static com.example.banking.eventsourcing.PayloadCodec testCodec() {
        return new com.example.banking.eventsourcing.PayloadCodec() {
            @Override public String encode(Object value) { return String.valueOf(((Counter) value).value()); }
            @Override @SuppressWarnings("unchecked")
            public <T> T decode(String json, Class<T> type) { return (T) new Counter(Integer.parseInt(json)); }
        };
    }
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/eventsourcing/EventSourcingRepositoryTest.java`:

```java
package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.support.CounterBehaviour;
import com.example.banking.eventsourcing.support.InMemoryEventStore;
import com.example.banking.eventsourcing.support.InMemorySnapshotStore;
import io.vavr.control.Either;
import org.junit.jupiter.api.Test;

import java.util.List;

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
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=EventSourcingRepositoryTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR — `CommittedEvents`, `EventSourcingRepository` do not exist.

- [ ] **Step 4: Implement**

`src/main/java/com/example/banking/eventsourcing/CommittedEvents.java`:

```java
package com.example.banking.eventsourcing;

import java.util.List;

/** The successful outcome of a command: the events appended and the stream's new head. */
public record CommittedEvents(String aggregateId, long lastSequenceNr, List<Object> events) {
}
```

`src/main/java/com/example/banking/eventsourcing/EventSourcingRepository.java`:

```java
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
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=EventSourcingRepositoryTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/test/java/com/example/banking/eventsourcing
git commit -m "feat: add event-sourcing repository with conflict retry and snapshot threshold"
```

---

### Task 6: CommandBus — striped, asynchronous, explicit registration

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/CommandHandler.java`
- Create: `src/main/java/com/example/banking/eventsourcing/CommandBus.java`
- Create: `src/main/java/com/example/banking/eventsourcing/StripedCommandBus.java`
- Test: `src/test/java/com/example/banking/eventsourcing/StripedCommandBusTest.java`

**Interfaces:**
- Consumes: `DomainError` (Task 1), `CommittedEvents`, `ConcurrencyConflict` (Tasks 2/5).
- Produces:
  - `interface CommandHandler<C> { io.vavr.control.Either<DomainError, CommittedEvents> handle(C command); }`
  - `interface CommandBus { <C> void register(Class<C> commandType, java.util.function.Function<C,String> aggregateIdOf, CommandHandler<C> handler); <C> java.util.concurrent.CompletableFuture<io.vavr.control.Either<DomainError, CommittedEvents>> dispatch(C command); }`
  - `class StripedCommandBus implements CommandBus, AutoCloseable` (constructor: `int stripes`). `dispatch` of an unregistered command returns a future failed with `IllegalArgumentException`; a `ConcurrencyConflict` thrown by the handler fails the future (callers map it to 409).

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/eventsourcing/StripedCommandBusTest.java`:

```java
package com.example.banking.eventsourcing;

import io.vavr.control.Either;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class StripedCommandBusTest {

    record TestCommand(String aggregateId, int seq) {}
    record OtherCommand(String aggregateId) {}

    private final StripedCommandBus bus = new StripedCommandBus(4);

    @AfterEach
    void tearDown() {
        bus.close();
    }

    @Test
    void sameAggregateCommandsExecuteInDispatchOrder() throws Exception {
        List<Integer> observed = new CopyOnWriteArrayList<>();
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> {
            observed.add(command.seq());
            return Either.right(new CommittedEvents(command.aggregateId(), command.seq(), List.of()));
        });

        List<CompletableFuture<Either<DomainError, CommittedEvents>>> futures =
                java.util.stream.IntStream.range(0, 100)
                        .mapToObj(i -> bus.dispatch(new TestCommand("same-aggregate", i)))
                        .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();

        assertThat(observed).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, 100).boxed().toList());
    }

    @Test
    void leftOutcomePassesThrough() throws Exception {
        DomainError error = new DomainError() {
            @Override public String code() { return "test.error"; }
            @Override public String message() { return "boom"; }
        };
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> Either.left(error));

        Either<DomainError, CommittedEvents> outcome = bus.dispatch(new TestCommand("a", 0)).get();

        assertThat(outcome.getLeft().code()).isEqualTo("test.error");
    }

    @Test
    void handlerExceptionFailsTheFuture() {
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> {
            throw new ConcurrencyConflict("a", 0);
        });

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> bus.dispatch(new TestCommand("a", 0)).get())
                .withCauseInstanceOf(ConcurrencyConflict.class);
    }

    @Test
    void unregisteredCommandFailsTheFuture() {
        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> bus.dispatch(new OtherCommand("a")).get())
                .withCauseInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=StripedCommandBusTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 3: Implement**

`src/main/java/com/example/banking/eventsourcing/CommandHandler.java`:

```java
package com.example.banking.eventsourcing;

import io.vavr.control.Either;

public interface CommandHandler<C> {
    Either<DomainError, CommittedEvents> handle(C command);
}
```

`src/main/java/com/example/banking/eventsourcing/CommandBus.java`:

```java
package com.example.banking.eventsourcing;

import io.vavr.control.Either;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Asynchronous in-process command dispatch. Registration is explicit; the aggregateIdOf function
 * routes each command to a stripe so same-aggregate commands execute serially in dispatch order.
 */
public interface CommandBus {

    <C> void register(Class<C> commandType, Function<C, String> aggregateIdOf, CommandHandler<C> handler);

    <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command);
}
```

`src/main/java/com/example/banking/eventsourcing/StripedCommandBus.java`:

```java
package com.example.banking.eventsourcing;

import io.vavr.control.Either;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public final class StripedCommandBus implements CommandBus, AutoCloseable {

    private record Registration<C>(Function<C, String> aggregateIdOf, CommandHandler<C> handler) {}

    private final ExecutorService[] stripes;
    private final Map<Class<?>, Registration<?>> registrations = new ConcurrentHashMap<>();

    public StripedCommandBus(int stripeCount) {
        this.stripes = new ExecutorService[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public <C> void register(Class<C> commandType, Function<C, String> aggregateIdOf, CommandHandler<C> handler) {
        registrations.put(commandType, new Registration<>(aggregateIdOf, handler));
    }

    @Override
    public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
        @SuppressWarnings("unchecked")
        Registration<C> registration = (Registration<C>) registrations.get(command.getClass());
        if (registration == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("no handler registered for " + command.getClass().getName()));
        }
        String aggregateId = registration.aggregateIdOf().apply(command);
        ExecutorService stripe = stripes[Math.floorMod(aggregateId.hashCode(), stripes.length)];
        return CompletableFuture.supplyAsync(() -> registration.handler().handle(command), stripe);
    }

    @Override
    public void close() {
        for (ExecutorService stripe : stripes) {
            stripe.shutdown();
        }
    }
}
```

- [ ] **Step 4: Run the test — verify it passes**

Run: `mvn -B test -Dtest=StripedCommandBusTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/test/java/com/example/banking/eventsourcing
git commit -m "feat: add striped asynchronous command bus with explicit registration"
```

---

### Task 7: TokenStore, TrackingProcessor, V4 migration

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/TokenStore.java`
- Create: `src/main/java/com/example/banking/eventsourcing/EventHandler.java`
- Create: `src/main/java/com/example/banking/eventsourcing/TrackingProcessor.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JdbcTokenStore.java`
- Create: `src/main/resources/db/migration/V4__tracking_token.sql`
- Test: `src/test/java/com/example/banking/infra/TrackingProcessorTest.java`

**Interfaces:**
- Consumes: `EventStore`, `StoredEvent`, `TransactionalRunner`, `JdbcEventStore`, `SpringTransactionalRunner` (Task 2).
- Produces:
  - `interface TokenStore { long load(String processorName); void save(String processorName, long position); void reset(String processorName); }` — `load` of an unknown processor returns `0` (start of stream) and creates the row.
  - `interface EventHandler { void handle(StoredEvent event); }`
  - `class TrackingProcessor` — constructor `(String name, EventStore eventStore, TokenStore tokenStore, TransactionalRunner tx, EventHandler handler, int batchSize, java.time.Duration pollInterval)`; methods `int processOnce()` (one transactional batch, returns count — the deterministic unit tests exercise), `void start()` / `void stop()` (daemon poll loop: re-polls immediately while batches are full, sleeps `pollInterval` when idle).
  - Rebuild = `tokenStore.reset(name)` while stopped (plus truncating the projection's own tables, owned by later milestones).

- [ ] **Step 1: Write the migration**

`src/main/resources/db/migration/V4__tracking_token.sql`:

```sql
-- One row per tracking processor: the last global_position it has applied.
CREATE TABLE tracking_token (
    processor_name VARCHAR(64) NOT NULL PRIMARY KEY,
    position       BIGINT      NOT NULL,
    updated_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/infra/TrackingProcessorTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.JdbcEventStore;
import com.example.banking.adapter.out.eventstore.JdbcTokenStore;
import com.example.banking.adapter.out.eventstore.SpringTransactionalRunner;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import com.example.banking.eventsourcing.TokenStore;
import com.example.banking.eventsourcing.TrackingProcessor;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@SpringBootTest
@Import(ContainersConfig.class)
class TrackingProcessorTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager txManager;

    EventStore eventStore;
    TokenStore tokenStore;
    TransactionalRunner tx;
    List<StoredEvent> seen;

    @BeforeEach
    void setUp() {
        tx = new SpringTransactionalRunner(new TransactionTemplate(txManager));
        eventStore = new JdbcEventStore(jdbc, tx);
        tokenStore = new JdbcTokenStore(jdbc);
        seen = new CopyOnWriteArrayList<>();
        jdbc.update("DELETE FROM domain_event");
        jdbc.update("DELETE FROM tracking_token");
        jdbc.update("UPDATE event_store_sequence SET next_position = 1");
    }

    private TrackingProcessor processor(String name) {
        return new TrackingProcessor(name, eventStore, tokenStore, tx, seen::add,
                2, Duration.ofMillis(50));
    }

    private void appendEvents(String aggregateId, int count) {
        for (int i = 0; i < count; i++) {
            eventStore.append("Counter", aggregateId, i - 1, List.of(new SerializedEvent(
                    UUID.randomUUID().toString(), "Incremented", 1, "{\"by\":1}", "{}", Instant.now())));
        }
    }

    @Test
    void processesBatchesAndAdvancesToken() {
        appendEvents("p-1", 5);
        TrackingProcessor processor = processor("test-proc");

        assertThat(processor.processOnce()).isEqualTo(2);
        assertThat(processor.processOnce()).isEqualTo(2);
        assertThat(processor.processOnce()).isEqualTo(1);
        assertThat(processor.processOnce()).isEqualTo(0);

        assertThat(seen).hasSize(5);
        assertThat(seen).extracting(StoredEvent::globalPosition).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(tokenStore.load("test-proc")).isEqualTo(5);
    }

    @Test
    void resumesFromPersistedTokenAfterRestart() {
        appendEvents("p-2", 3);
        processor("resume-proc").processOnce();  // applies 2, token = 2

        TrackingProcessor fresh = processor("resume-proc");
        fresh.processOnce();

        assertThat(seen).hasSize(3);
        assertThat(tokenStore.load("resume-proc")).isEqualTo(3);
    }

    @Test
    void handlerFailureRollsBackTokenAndBatchIsRetried() {
        appendEvents("p-3", 1);
        TrackingProcessor failing = new TrackingProcessor("fail-proc", eventStore, tokenStore, tx,
                event -> { throw new IllegalStateException("projection down"); },
                10, Duration.ofMillis(50));

        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(failing::processOnce);
        assertThat(tokenStore.load("fail-proc")).isZero();

        processor("fail-proc").processOnce();  // same name, healthy handler: event is redelivered
        assertThat(seen).hasSize(1);
    }

    @Test
    void resetReplaysFromStartOfStream() {
        appendEvents("p-4", 2);
        TrackingProcessor processor = processor("rebuild-proc");
        processor.processOnce();
        assertThat(seen).hasSize(2);

        tokenStore.reset("rebuild-proc");
        processor.processOnce();

        assertThat(seen).hasSize(4);  // replayed both events
    }
}
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=TrackingProcessorTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 4: Implement**

`src/main/java/com/example/banking/eventsourcing/TokenStore.java`:

```java
package com.example.banking.eventsourcing;

/** Persisted per-processor progress through the global event stream. */
public interface TokenStore {

    /** Last applied global position; 0 for a processor that has never run. */
    long load(String processorName);

    void save(String processorName, long position);

    /** Rebuild support: next load returns 0 and the processor replays from the start. */
    void reset(String processorName);
}
```

`src/main/java/com/example/banking/eventsourcing/EventHandler.java`:

```java
package com.example.banking.eventsourcing;

/** A projection/relay/saga hook invoked for each committed event, in global order. */
public interface EventHandler {
    void handle(StoredEvent event);
}
```

`src/main/java/com/example/banking/eventsourcing/TrackingProcessor.java`:

```java
package com.example.banking.eventsourcing;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Named poll loop over the event store. Handler effects and the token update run inside one
 * transaction, so MySQL-backed handlers get exactly-once semantics; handlers with external
 * effects must be idempotent (at-least-once).
 */
public final class TrackingProcessor {

    private final String name;
    private final EventStore eventStore;
    private final TokenStore tokenStore;
    private final TransactionalRunner tx;
    private final EventHandler handler;
    private final int batchSize;
    private final Duration pollInterval;
    private volatile boolean running;
    private Thread loop;

    public TrackingProcessor(String name, EventStore eventStore, TokenStore tokenStore,
                             TransactionalRunner tx, EventHandler handler,
                             int batchSize, Duration pollInterval) {
        this.name = name;
        this.eventStore = eventStore;
        this.tokenStore = tokenStore;
        this.tx = tx;
        this.handler = handler;
        this.batchSize = batchSize;
        this.pollInterval = pollInterval;
    }

    public String name() {
        return name;
    }

    /** One transactional batch: read after token, handle, advance token. Returns events applied. */
    public int processOnce() {
        AtomicInteger applied = new AtomicInteger();
        tx.inTransaction(() -> {
            long token = tokenStore.load(name);
            List<StoredEvent> batch = eventStore.readAllAfter(token, batchSize);
            for (StoredEvent event : batch) {
                handler.handle(event);
            }
            if (!batch.isEmpty()) {
                tokenStore.save(name, batch.get(batch.size() - 1).globalPosition());
            }
            applied.set(batch.size());
        });
        return applied.get();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = new Thread(this::runLoop, "tracking-processor-" + name);
        loop.setDaemon(true);
        loop.start();
    }

    public synchronized void stop() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    private void runLoop() {
        while (running) {
            int applied;
            try {
                applied = processOnce();
            } catch (RuntimeException e) {
                applied = 0;  // roll back and retry after the poll interval; the token did not move
            }
            if (applied < batchSize) {
                try {
                    Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/JdbcTokenStore.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.TokenStore;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcTokenStore implements TokenStore {

    private final JdbcTemplate jdbc;

    public JdbcTokenStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public long load(String processorName) {
        jdbc.update("INSERT IGNORE INTO tracking_token (processor_name, position) VALUES (?, 0)",
                processorName);
        return jdbc.queryForObject(
                "SELECT position FROM tracking_token WHERE processor_name = ? FOR UPDATE",
                Long.class, processorName);
    }

    @Override
    public void save(String processorName, long position) {
        jdbc.update("UPDATE tracking_token SET position = ? WHERE processor_name = ?",
                position, processorName);
    }

    @Override
    public void reset(String processorName) {
        jdbc.update("UPDATE tracking_token SET position = 0 WHERE processor_name = ?", processorName);
    }
}
```

Note: `load` uses `FOR UPDATE` so two instances of the same processor cannot apply a batch
concurrently — the second blocks on the row lock until the first commits, then reads the advanced
token. `FOR UPDATE` outside a transaction (the `reset` test path) is harmless.

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=TrackingProcessorTest > target/mvn-out.txt 2>&1; tail -n 40 target/mvn-out.txt`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/main/java/com/example/banking/adapter/out/eventstore src/main/resources/db/migration/V4__tracking_token.sql src/test/java/com/example/banking/infra/TrackingProcessorTest.java
git commit -m "feat: add token-based tracking processor with transactional exactly-once batches"
```

---

### Task 8: Saga runtime — SagaBehaviour, SagaStore, SagaManager, SagaTestFixture, V5 migration

**Files:**
- Create: `src/main/java/com/example/banking/eventsourcing/SagaBehaviour.java`
- Create: `src/main/java/com/example/banking/eventsourcing/SagaUpdate.java`
- Create: `src/main/java/com/example/banking/eventsourcing/DeadlineRequest.java`
- Create: `src/main/java/com/example/banking/eventsourcing/SagaInstance.java`
- Create: `src/main/java/com/example/banking/eventsourcing/SagaStore.java`
- Create: `src/main/java/com/example/banking/eventsourcing/DeadlineScheduler.java`
- Create: `src/main/java/com/example/banking/eventsourcing/SagaManager.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JdbcSagaStore.java`
- Create: `src/main/resources/db/migration/V5__saga.sql`
- Create: `src/test/java/com/example/banking/eventsourcing/fixture/SagaTestFixture.java`
- Create: `src/test/java/com/example/banking/eventsourcing/support/TransferLikeSaga.java`
- Test: `src/test/java/com/example/banking/eventsourcing/SagaTestFixtureTest.java`
- Test: `src/test/java/com/example/banking/infra/SagaManagerTest.java`

**Interfaces:**
- Consumes: `EventHandler`, `StoredEvent` (Tasks 2/7), `EventSerializer`, `PayloadCodec` (Task 3), `CommandBus` (Task 6).
- Produces:
  - `interface SagaBehaviour<S> { String sagaType(); java.util.Optional<String> associationKey(Object event); boolean startsSaga(Object event); S initial(String associationKey); SagaUpdate<S> react(S state, Object event); }`
  - `record SagaUpdate<S>(S state, boolean terminal, List<Object> commands, List<DeadlineRequest> schedule, List<String> cancelDeadlines)` with factory `SagaUpdate.of(state)` (non-terminal, empty lists) and fluent copies `withCommands(...)`, `withSchedule(...)`, `withCancel(...)`, `asTerminal()`.
  - `record DeadlineRequest(String deadlineId, java.time.Duration after, Object payload)`
  - `record SagaInstance(String sagaId, String sagaType, String statePayload, boolean terminal)`
  - `interface SagaStore { java.util.Optional<SagaInstance> findByAssociation(String sagaType, String associationKey); java.util.Optional<SagaInstance> findById(String sagaId); void insert(SagaInstance saga, String associationKey); void save(SagaInstance saga); }`
  - `interface DeadlineScheduler { void schedule(String sagaType, String sagaId, DeadlineRequest request); void cancel(String deadlineId); }` (JDBC implementation arrives in Task 9; Task 8 tests use a recording fake)
  - `class SagaManager<S> implements EventHandler` — constructor `(SagaBehaviour<S> behaviour, SagaStore store, PayloadCodec codec, Class<S> stateType, EventSerializer serializer, CommandBus commandBus, DeadlineScheduler deadlines)`; also `void handleDeadline(String sagaId, Object deadlineEvent)` (used by Task 9's poller).
  - `SagaTestFixture` — pure GWT for sagas.

- [ ] **Step 1: Write the migration**

`src/main/resources/db/migration/V5__saga.sql`:

```sql
CREATE TABLE saga_instance (
    saga_id    CHAR(36)    NOT NULL PRIMARY KEY,
    saga_type  VARCHAR(64) NOT NULL,
    state      JSON        NOT NULL,
    terminal   BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE saga_association (
    saga_type       VARCHAR(64)  NOT NULL,
    association_key VARCHAR(128) NOT NULL,
    saga_id         CHAR(36)     NOT NULL,
    PRIMARY KEY (saga_type, association_key)
);
```

- [ ] **Step 2: Write the toy saga and the failing fixture test**

`src/test/java/com/example/banking/eventsourcing/support/TransferLikeSaga.java`:

```java
package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.SagaBehaviour;
import com.example.banking.eventsourcing.SagaUpdate;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Toy saga mirroring the transfer flow: request -> debit -> credit, refund on failure, timeout. */
public final class TransferLikeSaga implements SagaBehaviour<TransferLikeSaga.State> {

    public record Requested(String txId) {}
    public record Debited(String txId) {}
    public record Credited(String txId) {}
    public record CreditFailed(String txId) {}
    public record TimedOut(String txId) {}

    public record DebitCmd(String txId) {}
    public record CreditCmd(String txId) {}
    public record RefundCmd(String txId) {}

    public enum Phase { DEBITING, CREDITING, DONE }
    public record State(String txId, Phase phase) {}

    @Override public String sagaType() { return "TransferLike"; }

    @Override public Optional<String> associationKey(Object event) {
        return switch (event) {
            case Requested e -> Optional.of(e.txId());
            case Debited e -> Optional.of(e.txId());
            case Credited e -> Optional.of(e.txId());
            case CreditFailed e -> Optional.of(e.txId());
            case TimedOut e -> Optional.of(e.txId());
            default -> Optional.empty();
        };
    }

    @Override public boolean startsSaga(Object event) { return event instanceof Requested; }

    @Override public State initial(String associationKey) {
        return new State(associationKey, Phase.DEBITING);
    }

    @Override public SagaUpdate<State> react(State state, Object event) {
        return switch (event) {
            case Requested e -> SagaUpdate.of(state)
                    .withCommands(List.of(new DebitCmd(e.txId())))
                    .withSchedule(List.of(new DeadlineRequest(
                            "timeout-" + e.txId(), Duration.ofMinutes(5), new TimedOut(e.txId()))));
            case Debited e -> SagaUpdate.of(new State(state.txId(), Phase.CREDITING))
                    .withCommands(List.of(new CreditCmd(e.txId())));
            case Credited e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCancel(List.of("timeout-" + e.txId())).asTerminal();
            case CreditFailed e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCommands(List.of(new RefundCmd(e.txId())))
                    .withCancel(List.of("timeout-" + e.txId())).asTerminal();
            case TimedOut e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCommands(List.of(new RefundCmd(e.txId()))).asTerminal();
            default -> SagaUpdate.of(state);
        };
    }
}
```

`src/test/java/com/example/banking/eventsourcing/SagaTestFixtureTest.java`:

```java
package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.fixture.SagaTestFixture;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import org.junit.jupiter.api.Test;

import static com.example.banking.eventsourcing.support.TransferLikeSaga.*;

class SagaTestFixtureTest {

    private final SagaTestFixture<TransferLikeSaga.State> fixture =
            SagaTestFixture.forBehaviour(new TransferLikeSaga());

    @Test
    void happyPath_creditAfterDebit() {
        fixture.given(new Requested("t-1"), new Debited("t-1"))
                .whenEvent(new Credited("t-1"))
                .expectNoCommands()
                .expectCancelled("timeout-t-1")
                .expectTerminal();
    }

    @Test
    void compensation_refundOnCreditFailure() {
        fixture.given(new Requested("t-2"), new Debited("t-2"))
                .whenEvent(new CreditFailed("t-2"))
                .expectDispatched(new RefundCmd("t-2"))
                .expectTerminal();
    }

    @Test
    void start_schedulesTimeoutDeadline() {
        fixture.givenNoPriorActivity()
                .whenEvent(new Requested("t-3"))
                .expectDispatched(new DebitCmd("t-3"))
                .expectScheduled("timeout-t-3");
    }
}
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=SagaTestFixtureTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 4: Implement the kernel saga types**

`src/main/java/com/example/banking/eventsourcing/DeadlineRequest.java`:

```java
package com.example.banking.eventsourcing;

import java.time.Duration;

/** A timeout the saga wants delivered back to itself as `payload` after `after` elapses. */
public record DeadlineRequest(String deadlineId, Duration after, Object payload) {
}
```

`src/main/java/com/example/banking/eventsourcing/SagaUpdate.java`:

```java
package com.example.banking.eventsourcing;

import java.util.List;

/** The outcome of one saga reaction: new state plus the effects to perform. */
public record SagaUpdate<S>(
        S state,
        boolean terminal,
        List<Object> commands,
        List<DeadlineRequest> schedule,
        List<String> cancelDeadlines) {

    public static <S> SagaUpdate<S> of(S state) {
        return new SagaUpdate<>(state, false, List.of(), List.of(), List.of());
    }

    public SagaUpdate<S> withCommands(List<Object> newCommands) {
        return new SagaUpdate<>(state, terminal, List.copyOf(newCommands), schedule, cancelDeadlines);
    }

    public SagaUpdate<S> withSchedule(List<DeadlineRequest> newSchedule) {
        return new SagaUpdate<>(state, terminal, commands, List.copyOf(newSchedule), cancelDeadlines);
    }

    public SagaUpdate<S> withCancel(List<String> newCancels) {
        return new SagaUpdate<>(state, terminal, commands, schedule, List.copyOf(newCancels));
    }

    public SagaUpdate<S> asTerminal() {
        return new SagaUpdate<>(state, true, commands, schedule, cancelDeadlines);
    }
}
```

`src/main/java/com/example/banking/eventsourcing/SagaBehaviour.java`:

```java
package com.example.banking.eventsourcing;

import java.util.Optional;

/** Pure saga state machine: correlate events, react with new state and effects. */
public interface SagaBehaviour<S> {

    String sagaType();

    /** The correlation value for this event, or empty when the event is not this saga's concern. */
    Optional<String> associationKey(Object event);

    boolean startsSaga(Object event);

    S initial(String associationKey);

    SagaUpdate<S> react(S state, Object event);
}
```

`src/main/java/com/example/banking/eventsourcing/SagaInstance.java`:

```java
package com.example.banking.eventsourcing;

/** Persisted saga state. statePayload is the codec-encoded saga state. */
public record SagaInstance(String sagaId, String sagaType, String statePayload, boolean terminal) {
}
```

`src/main/java/com/example/banking/eventsourcing/SagaStore.java`:

```java
package com.example.banking.eventsourcing;

import java.util.Optional;

public interface SagaStore {

    Optional<SagaInstance> findByAssociation(String sagaType, String associationKey);

    Optional<SagaInstance> findById(String sagaId);

    void insert(SagaInstance saga, String associationKey);

    void save(SagaInstance saga);
}
```

`src/main/java/com/example/banking/eventsourcing/DeadlineScheduler.java`:

```java
package com.example.banking.eventsourcing;

/** Port for saga timeouts. The JDBC implementation and its poller live in the adapter. */
public interface DeadlineScheduler {

    void schedule(String sagaType, String sagaId, DeadlineRequest request);

    void cancel(String deadlineId);
}
```

`src/main/java/com/example/banking/eventsourcing/SagaManager.java`:

```java
package com.example.banking.eventsourcing;

import java.util.UUID;

/**
 * Runs a saga behaviour as an event handler on a tracking processor: correlate, load or start,
 * react, persist, then perform effects (dispatch commands, schedule/cancel deadlines).
 * Effects are at-least-once on redelivery; commands must be idempotent downstream.
 */
public final class SagaManager<S> implements EventHandler {

    private final SagaBehaviour<S> behaviour;
    private final SagaStore store;
    private final PayloadCodec codec;
    private final Class<S> stateType;
    private final EventSerializer serializer;
    private final CommandBus commandBus;
    private final DeadlineScheduler deadlines;

    public SagaManager(SagaBehaviour<S> behaviour, SagaStore store, PayloadCodec codec,
                       Class<S> stateType, EventSerializer serializer,
                       CommandBus commandBus, DeadlineScheduler deadlines) {
        this.behaviour = behaviour;
        this.store = store;
        this.codec = codec;
        this.stateType = stateType;
        this.serializer = serializer;
        this.commandBus = commandBus;
        this.deadlines = deadlines;
    }

    @Override
    public void handle(StoredEvent stored) {
        Object event = serializer.deserialize(stored.event());
        behaviour.associationKey(event).ifPresent(key -> handleCorrelated(key, event));
    }

    /** Deadline delivery path: the poller knows the saga id directly, no association lookup. */
    public void handleDeadline(String sagaId, Object deadlineEvent) {
        store.findById(sagaId)
                .filter(instance -> !instance.terminal())
                .ifPresent(instance -> reactAndPersist(instance, deadlineEvent));
    }

    private void handleCorrelated(String key, Object event) {
        SagaInstance instance = store.findByAssociation(behaviour.sagaType(), key).orElse(null);
        if (instance == null) {
            if (!behaviour.startsSaga(event)) {
                return;
            }
            instance = new SagaInstance(UUID.randomUUID().toString(), behaviour.sagaType(),
                    codec.encode(behaviour.initial(key)), false);
            store.insert(instance, key);
        }
        if (instance.terminal()) {
            return;
        }
        reactAndPersist(instance, event);
    }

    private void reactAndPersist(SagaInstance instance, Object event) {
        S state = codec.decode(instance.statePayload(), stateType);
        SagaUpdate<S> update = behaviour.react(state, event);
        store.save(new SagaInstance(instance.sagaId(), instance.sagaType(),
                codec.encode(update.state()), update.terminal()));
        update.cancelDeadlines().forEach(deadlines::cancel);
        update.schedule().forEach(request ->
                deadlines.schedule(behaviour.sagaType(), instance.sagaId(), request));
        update.commands().forEach(commandBus::dispatch);
    }
}
```

- [ ] **Step 5: Implement the saga fixture**

`src/test/java/com/example/banking/eventsourcing/fixture/SagaTestFixture.java`:

```java
package com.example.banking.eventsourcing.fixture;

import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.SagaBehaviour;
import com.example.banking.eventsourcing.SagaUpdate;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure given/when/then for sagas: folds given events through react, asserts the last update. */
public final class SagaTestFixture<S> {

    private final SagaBehaviour<S> behaviour;
    private S state;
    private boolean started;

    private SagaTestFixture(SagaBehaviour<S> behaviour) {
        this.behaviour = behaviour;
    }

    public static <S> SagaTestFixture<S> forBehaviour(SagaBehaviour<S> behaviour) {
        return new SagaTestFixture<>(behaviour);
    }

    public SagaTestFixture<S> givenNoPriorActivity() {
        return this;
    }

    public SagaTestFixture<S> given(Object... events) {
        for (Object event : events) {
            apply(event);
        }
        return this;
    }

    public Then whenEvent(Object event) {
        return new Then(apply(event));
    }

    private SagaUpdate<S> apply(Object event) {
        String key = behaviour.associationKey(event)
                .orElseThrow(() -> new AssertionError("event has no association key: " + event));
        if (!started) {
            assertThat(behaviour.startsSaga(event))
                    .as("first event must start the saga: %s", event).isTrue();
            state = behaviour.initial(key);
            started = true;
        }
        SagaUpdate<S> update = behaviour.react(state, event);
        state = update.state();
        return update;
    }

    public final class Then {
        private final SagaUpdate<S> update;

        private Then(SagaUpdate<S> update) {
            this.update = update;
        }

        public Then expectDispatched(Object... commands) {
            assertThat(update.commands()).containsExactly(commands);
            return this;
        }

        public Then expectNoCommands() {
            assertThat(update.commands()).isEmpty();
            return this;
        }

        public Then expectScheduled(String... deadlineIds) {
            assertThat(update.schedule()).extracting(DeadlineRequest::deadlineId)
                    .containsExactly(deadlineIds);
            return this;
        }

        public Then expectCancelled(String... deadlineIds) {
            assertThat(update.cancelDeadlines()).containsExactly(deadlineIds);
            return this;
        }

        public Then expectTerminal() {
            assertThat(update.terminal()).as("saga should be terminal").isTrue();
            return this;
        }
    }
}
```

- [ ] **Step 6: Run the fixture test — verify it passes**

Run: `mvn -B test -Dtest=SagaTestFixtureTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (3 tests).

- [ ] **Step 7: Write the failing SagaManager integration test**

`src/test/java/com/example/banking/infra/SagaManagerTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.JdbcSagaStore;
import com.example.banking.eventsourcing.CommandBus;
import com.example.banking.eventsourcing.CommandHandler;
import com.example.banking.eventsourcing.CommittedEvents;
import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.DeadlineScheduler;
import com.example.banking.eventsourcing.DomainError;
import com.example.banking.eventsourcing.EventSerializer;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.SagaManager;
import com.example.banking.eventsourcing.SagaStore;
import com.example.banking.eventsourcing.SerializedEvent;
import com.example.banking.eventsourcing.StoredEvent;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import com.example.banking.adapter.out.eventstore.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.UpcasterChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Either;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static com.example.banking.eventsourcing.support.TransferLikeSaga.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class SagaManagerTest {

    @Autowired JdbcTemplate jdbc;

    SagaStore sagaStore;
    SagaManager<TransferLikeSaga.State> manager;
    List<Object> dispatched;
    List<String> scheduled;
    List<String> cancelled;
    EventSerializer serializer;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM saga_instance");
        jdbc.update("DELETE FROM saga_association");
        ObjectMapper mapper = new ObjectMapper();
        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("Requested", 1, Requested.class);
        registry.register("Debited", 1, Debited.class);
        registry.register("Credited", 1, Credited.class);
        serializer = new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), Clock.systemUTC());
        dispatched = new CopyOnWriteArrayList<>();
        scheduled = new CopyOnWriteArrayList<>();
        cancelled = new CopyOnWriteArrayList<>();
        CommandBus recordingBus = new CommandBus() {
            @Override public <C> void register(Class<C> type, Function<C, String> idOf, CommandHandler<C> handler) {}
            @Override public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
                dispatched.add(command);
                return CompletableFuture.completedFuture(Either.right(new CommittedEvents("x", 0, List.of())));
            }
        };
        DeadlineScheduler recordingDeadlines = new DeadlineScheduler() {
            @Override public void schedule(String sagaType, String sagaId, DeadlineRequest request) {
                scheduled.add(request.deadlineId());
            }
            @Override public void cancel(String deadlineId) { cancelled.add(deadlineId); }
        };
        sagaStore = new JdbcSagaStore(jdbc);
        manager = new SagaManager<>(new TransferLikeSaga(), sagaStore, new JacksonPayloadCodec(mapper),
                TransferLikeSaga.State.class, serializer, recordingBus, recordingDeadlines);
    }

    private StoredEvent stored(Object event, long position) {
        SerializedEvent serialized = serializer.serialize(event, Map.of());
        return new StoredEvent(position, "Transfer", "t-agg", position - 1, serialized);
    }

    @Test
    void startsPersistsCorrelatesAndFinishesASaga() {
        manager.handle(stored(new Requested("t-1"), 1));

        assertThat(dispatched).containsExactly(new DebitCmd("t-1"));
        assertThat(scheduled).containsExactly("timeout-t-1");
        assertThat(sagaStore.findByAssociation("TransferLike", "t-1")).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isFalse());

        manager.handle(stored(new Debited("t-1"), 2));
        manager.handle(stored(new Credited("t-1"), 3));

        assertThat(dispatched).containsExactly(new DebitCmd("t-1"), new CreditCmd("t-1"));
        assertThat(cancelled).containsExactly("timeout-t-1");
        assertThat(sagaStore.findByAssociation("TransferLike", "t-1")).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isTrue());
    }

    @Test
    void terminalSagaIgnoresFurtherEvents() {
        manager.handle(stored(new Requested("t-2"), 1));
        manager.handle(stored(new Debited("t-2"), 2));
        manager.handle(stored(new Credited("t-2"), 3));
        int commandCount = dispatched.size();

        manager.handle(stored(new Debited("t-2"), 4));

        assertThat(dispatched).hasSize(commandCount);
    }

    @Test
    void nonStartingEventWithoutExistingSagaIsIgnored() {
        manager.handle(stored(new Debited("t-3"), 1));

        assertThat(dispatched).isEmpty();
        assertThat(sagaStore.findByAssociation("TransferLike", "t-3")).isEmpty();
    }
}
```

- [ ] **Step 8: Implement the JDBC saga store**

`src/main/java/com/example/banking/adapter/out/eventstore/JdbcSagaStore.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.SagaInstance;
import com.example.banking.eventsourcing.SagaStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Optional;

public final class JdbcSagaStore implements SagaStore {

    private static final RowMapper<SagaInstance> ROW_MAPPER = (rs, rowNum) -> new SagaInstance(
            rs.getString("saga_id"),
            rs.getString("saga_type"),
            rs.getString("state"),
            rs.getBoolean("terminal"));

    private final JdbcTemplate jdbc;

    public JdbcSagaStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<SagaInstance> findByAssociation(String sagaType, String associationKey) {
        return jdbc.query("""
                        SELECT s.* FROM saga_instance s
                        JOIN saga_association a ON a.saga_id = s.saga_id
                        WHERE a.saga_type = ? AND a.association_key = ?""",
                ROW_MAPPER, sagaType, associationKey).stream().findFirst();
    }

    @Override
    public Optional<SagaInstance> findById(String sagaId) {
        return jdbc.query("SELECT * FROM saga_instance WHERE saga_id = ?", ROW_MAPPER, sagaId)
                .stream().findFirst();
    }

    @Override
    public void insert(SagaInstance saga, String associationKey) {
        jdbc.update("INSERT INTO saga_instance (saga_id, saga_type, state, terminal) VALUES (?, ?, ?, ?)",
                saga.sagaId(), saga.sagaType(), saga.statePayload(), saga.terminal());
        jdbc.update("INSERT INTO saga_association (saga_type, association_key, saga_id) VALUES (?, ?, ?)",
                saga.sagaType(), associationKey, saga.sagaId());
    }

    @Override
    public void save(SagaInstance saga) {
        jdbc.update("UPDATE saga_instance SET state = ?, terminal = ? WHERE saga_id = ?",
                saga.statePayload(), saga.terminal(), saga.sagaId());
    }
}
```

- [ ] **Step 9: Run both tests — verify they pass**

Run: `mvn -B test -Dtest='SagaTestFixtureTest,SagaManagerTest' > target/mvn-out.txt 2>&1; tail -n 40 target/mvn-out.txt`
Expected: PASS (6 tests).

- [ ] **Step 10: Commit**

```bash
git add src/main/java/com/example/banking/eventsourcing src/main/java/com/example/banking/adapter/out/eventstore src/main/resources/db/migration/V5__saga.sql src/test/java/com/example/banking/eventsourcing src/test/java/com/example/banking/infra/SagaManagerTest.java
git commit -m "feat: add saga runtime with association-based correlation and test fixture"
```

---

### Task 9: Deadlines — JDBC scheduler, poller, V6 migration

**Files:**
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/JdbcDeadlineScheduler.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/DeadlinePoller.java`
- Create: `src/main/resources/db/migration/V6__deadline.sql`
- Test: `src/test/java/com/example/banking/infra/DeadlineTest.java`

**Interfaces:**
- Consumes: `DeadlineScheduler`, `DeadlineRequest`, `SagaManager.handleDeadline(String sagaId, Object deadlineEvent)` (Task 8); `EventTypeRegistry`, `PayloadCodec` (Task 3); `TransactionalRunner` (Task 2).
- Produces:
  - `class JdbcDeadlineScheduler implements DeadlineScheduler` — constructor `(JdbcTemplate, PayloadCodec, EventTypeRegistry, java.time.Clock)`. Deadline payload classes must be registered in the `EventTypeRegistry` (name used as `payload_type`).
  - `class DeadlinePoller` — constructor `(JdbcTemplate, TransactionalRunner, PayloadCodec, EventTypeRegistry, java.util.Map<String, SagaManager<?>> managersBySagaType, java.time.Clock, java.time.Duration pollInterval)`; methods `int pollOnce()` (transactional: deliver due deadlines, delete rows, return count) and `start()`/`stop()` (daemon loop like `TrackingProcessor`).

- [ ] **Step 1: Write the migration**

`src/main/resources/db/migration/V6__deadline.sql`:

```sql
-- Scheduled saga timeouts; delivered by DeadlinePoller and deleted on delivery or cancel.
CREATE TABLE deadline (
    deadline_id  VARCHAR(128) NOT NULL PRIMARY KEY,
    saga_type    VARCHAR(64)  NOT NULL,
    saga_id      CHAR(36)     NOT NULL,
    due_at       TIMESTAMP(6) NOT NULL,
    payload_type VARCHAR(128) NOT NULL,
    payload      JSON         NOT NULL,
    KEY idx_deadline_due (due_at)
);
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/infra/DeadlineTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.DeadlinePoller;
import com.example.banking.adapter.out.eventstore.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.JdbcDeadlineScheduler;
import com.example.banking.adapter.out.eventstore.JdbcSagaStore;
import com.example.banking.adapter.out.eventstore.SpringTransactionalRunner;
import com.example.banking.adapter.out.eventstore.UpcasterChain;
import com.example.banking.eventsourcing.CommandBus;
import com.example.banking.eventsourcing.CommandHandler;
import com.example.banking.eventsourcing.CommittedEvents;
import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.DomainError;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import com.example.banking.eventsourcing.SagaInstance;
import com.example.banking.eventsourcing.SagaManager;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Either;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static com.example.banking.eventsourcing.support.TransferLikeSaga.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class DeadlineTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager txManager;

    Clock clock = Clock.fixed(Instant.parse("2026-07-24T10:00:00Z"), ZoneOffset.UTC);
    PayloadCodec codec;
    EventTypeRegistry registry;
    JdbcDeadlineScheduler scheduler;
    DeadlinePoller poller;
    JdbcSagaStore sagaStore;
    List<Object> dispatched;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM deadline");
        jdbc.update("DELETE FROM saga_instance");
        jdbc.update("DELETE FROM saga_association");
        ObjectMapper mapper = new ObjectMapper();
        codec = new JacksonPayloadCodec(mapper);
        registry = new EventTypeRegistry();
        registry.register("TimedOut", 1, TimedOut.class);
        scheduler = new JdbcDeadlineScheduler(jdbc, codec, registry, clock);
        sagaStore = new JdbcSagaStore(jdbc);
        dispatched = new CopyOnWriteArrayList<>();
        CommandBus recordingBus = new CommandBus() {
            @Override public <C> void register(Class<C> type, Function<C, String> idOf, CommandHandler<C> handler) {}
            @Override public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
                dispatched.add(command);
                return CompletableFuture.completedFuture(Either.right(new CommittedEvents("x", 0, List.of())));
            }
        };
        SagaManager<TransferLikeSaga.State> manager = new SagaManager<>(new TransferLikeSaga(),
                sagaStore, codec, TransferLikeSaga.State.class,
                new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), clock),
                recordingBus, scheduler);
        poller = new DeadlinePoller(jdbc, new SpringTransactionalRunner(new TransactionTemplate(txManager)),
                codec, registry, Map.of("TransferLike", manager), clock, Duration.ofMillis(50));
    }

    private String activeSaga(String txId) {
        String sagaId = java.util.UUID.randomUUID().toString();
        sagaStore.insert(new SagaInstance(sagaId, "TransferLike",
                codec.encode(new State(txId, Phase.CREDITING)), false), txId);
        return sagaId;
    }

    @Test
    void dueDeadlineIsDeliveredToTheSagaAndDeleted() {
        String sagaId = activeSaga("t-1");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-1", Duration.ZERO, new TimedOut("t-1")));

        int delivered = poller.pollOnce();

        assertThat(delivered).isEqualTo(1);
        assertThat(dispatched).containsExactly(new RefundCmd("t-1"));
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deadline", Long.class)).isZero();
        assertThat(sagaStore.findById(sagaId)).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isTrue());
    }

    @Test
    void futureDeadlineIsNotDelivered() {
        String sagaId = activeSaga("t-2");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-2", Duration.ofMinutes(5), new TimedOut("t-2")));

        assertThat(poller.pollOnce()).isZero();
        assertThat(dispatched).isEmpty();
    }

    @Test
    void cancelledDeadlineIsNeverDelivered() {
        String sagaId = activeSaga("t-3");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-3", Duration.ZERO, new TimedOut("t-3")));
        scheduler.cancel("timeout-t-3");

        assertThat(poller.pollOnce()).isZero();
        assertThat(dispatched).isEmpty();
    }

    @Test
    void deadlineForTerminalSagaIsDeletedWithoutEffect() {
        String sagaId = java.util.UUID.randomUUID().toString();
        sagaStore.insert(new SagaInstance(sagaId, "TransferLike",
                codec.encode(new State("t-4", Phase.DONE)), true), "t-4");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-4", Duration.ZERO, new TimedOut("t-4")));

        poller.pollOnce();

        assertThat(dispatched).isEmpty();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deadline", Long.class)).isZero();
    }
}
```

- [ ] **Step 3: Run the test — verify it fails to compile**

Run: `mvn -B test -Dtest=DeadlineTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: COMPILATION ERROR.

- [ ] **Step 4: Implement**

`src/main/java/com/example/banking/adapter/out/eventstore/JdbcDeadlineScheduler.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.DeadlineRequest;
import com.example.banking.eventsourcing.DeadlineScheduler;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;

public final class JdbcDeadlineScheduler implements DeadlineScheduler {

    private final JdbcTemplate jdbc;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Clock clock;

    public JdbcDeadlineScheduler(JdbcTemplate jdbc, PayloadCodec codec,
                                 EventTypeRegistry registry, Clock clock) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.registry = registry;
        this.clock = clock;
    }

    @Override
    public void schedule(String sagaType, String sagaId, DeadlineRequest request) {
        jdbc.update("""
                        INSERT INTO deadline (deadline_id, saga_type, saga_id, due_at, payload_type, payload)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE due_at = VALUES(due_at), payload = VALUES(payload)""",
                request.deadlineId(), sagaType, sagaId,
                Timestamp.from(clock.instant().plus(request.after())),
                registry.byClass(request.payload().getClass()).name(),
                codec.encode(request.payload()));
    }

    @Override
    public void cancel(String deadlineId) {
        jdbc.update("DELETE FROM deadline WHERE deadline_id = ?", deadlineId);
    }
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/DeadlinePoller.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import com.example.banking.eventsourcing.SagaManager;
import com.example.banking.eventsourcing.TransactionalRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Delivers due deadlines to their saga manager and deletes them, one transaction per poll. */
public final class DeadlinePoller {

    private record DueDeadline(String deadlineId, String sagaType, String sagaId,
                               String payloadType, String payload) {}

    private final JdbcTemplate jdbc;
    private final TransactionalRunner tx;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Map<String, SagaManager<?>> managersBySagaType;
    private final Clock clock;
    private final Duration pollInterval;
    private volatile boolean running;
    private Thread loop;

    public DeadlinePoller(JdbcTemplate jdbc, TransactionalRunner tx, PayloadCodec codec,
                          EventTypeRegistry registry, Map<String, SagaManager<?>> managersBySagaType,
                          Clock clock, Duration pollInterval) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
        this.registry = registry;
        this.managersBySagaType = Map.copyOf(managersBySagaType);
        this.clock = clock;
        this.pollInterval = pollInterval;
    }

    /** One transactional pass: deliver every due deadline, delete it, return the count. */
    public int pollOnce() {
        AtomicInteger delivered = new AtomicInteger();
        tx.inTransaction(() -> {
            List<DueDeadline> due = jdbc.query("""
                            SELECT deadline_id, saga_type, saga_id, payload_type, payload
                            FROM deadline WHERE due_at <= ?
                            ORDER BY due_at LIMIT 100 FOR UPDATE""",
                    (rs, rowNum) -> new DueDeadline(rs.getString("deadline_id"), rs.getString("saga_type"),
                            rs.getString("saga_id"), rs.getString("payload_type"), rs.getString("payload")),
                    Timestamp.from(clock.instant()));
            for (DueDeadline deadline : due) {
                SagaManager<?> manager = managersBySagaType.get(deadline.sagaType());
                if (manager != null) {
                    Object payload = codec.decode(deadline.payload(),
                            registry.byName(deadline.payloadType()).type());
                    manager.handleDeadline(deadline.sagaId(), payload);
                }
                jdbc.update("DELETE FROM deadline WHERE deadline_id = ?", deadline.deadlineId());
                delivered.incrementAndGet();
            }
        });
        return delivered.get();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = new Thread(this::runLoop, "deadline-poller");
        loop.setDaemon(true);
        loop.start();
    }

    public synchronized void stop() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    private void runLoop() {
        while (running) {
            try {
                pollOnce();
            } catch (RuntimeException e) {
                // roll back; due deadlines stay in the table and are retried next pass
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
```

Note: `registry.byName(...).type()` returns `Class<?>`; `codec.decode` accepts it as
`Class<T>` with `T` inferred as the wildcard — assign to `Object` as shown.

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=DeadlineTest > target/mvn-out.txt 2>&1; tail -n 40 target/mvn-out.txt`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/banking/adapter/out/eventstore src/main/resources/db/migration/V6__deadline.sql src/test/java/com/example/banking/infra/DeadlineTest.java
git commit -m "feat: add saga deadline scheduling and transactional delivery poller"
```

---

### Task 10: Spring wiring — properties, configuration, boot test

**Files:**
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/EventSourcingProperties.java`
- Create: `src/main/java/com/example/banking/adapter/out/eventstore/EventSourcingConfig.java`
- Modify: `src/main/resources/application.yml` (append the `banking.eventsourcing` block)
- Test: `src/test/java/com/example/banking/infra/KernelWiringTest.java`

**Interfaces:**
- Consumes: everything built in Tasks 2–9.
- Produces: application beans — `EventStore`, `SnapshotStore`, `TokenStore`, `SagaStore`, `EventSerializer`, `PayloadCodec`, `EventTypeRegistry`, `UpcasterChain`, `TransactionalRunner`, `CommandBus`, `DeadlineScheduler`, `Clock` — that later milestones inject to build repositories, processors, and sagas. `TrackingProcessor`/`SagaManager`/`DeadlinePoller` instances are **not** beans yet: they are constructed by the milestones that own concrete projections and sagas.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/infra/KernelWiringTest.java`:

```java
package com.example.banking.infra;

import com.example.banking.eventsourcing.CommandBus;
import com.example.banking.eventsourcing.DeadlineScheduler;
import com.example.banking.eventsourcing.EventSerializer;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.SagaStore;
import com.example.banking.eventsourcing.SnapshotStore;
import com.example.banking.eventsourcing.TokenStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class KernelWiringTest {

    @Autowired EventStore eventStore;
    @Autowired SnapshotStore snapshotStore;
    @Autowired TokenStore tokenStore;
    @Autowired SagaStore sagaStore;
    @Autowired EventSerializer eventSerializer;
    @Autowired CommandBus commandBus;
    @Autowired DeadlineScheduler deadlineScheduler;

    @Test
    void kernelBeansAreWired() {
        assertThat(eventStore).isNotNull();
        assertThat(snapshotStore).isNotNull();
        assertThat(tokenStore).isNotNull();
        assertThat(sagaStore).isNotNull();
        assertThat(eventSerializer).isNotNull();
        assertThat(commandBus).isNotNull();
        assertThat(deadlineScheduler).isNotNull();
    }
}
```

- [ ] **Step 2: Run the test — verify it fails**

Run: `mvn -B test -Dtest=KernelWiringTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: FAIL — `NoSuchBeanDefinitionException` for `EventStore`.

- [ ] **Step 3: Implement properties and configuration**

`src/main/java/com/example/banking/adapter/out/eventstore/EventSourcingProperties.java`:

```java
package com.example.banking.adapter.out.eventstore;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "banking.eventsourcing")
public record EventSourcingProperties(
        @DefaultValue("8") int commandStripes,
        @DefaultValue("100") int batchSize,
        @DefaultValue("100ms") Duration pollInterval,
        @DefaultValue("100") int snapshotThreshold,
        @DefaultValue("3") int maxCommandAttempts,
        @DefaultValue("500ms") Duration deadlinePollInterval) {
}
```

`src/main/java/com/example/banking/adapter/out/eventstore/EventSourcingConfig.java`:

```java
package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.CommandBus;
import com.example.banking.eventsourcing.DeadlineScheduler;
import com.example.banking.eventsourcing.EventSerializer;
import com.example.banking.eventsourcing.EventStore;
import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.PayloadCodec;
import com.example.banking.eventsourcing.SagaStore;
import com.example.banking.eventsourcing.SnapshotStore;
import com.example.banking.eventsourcing.StripedCommandBus;
import com.example.banking.eventsourcing.TokenStore;
import com.example.banking.eventsourcing.TransactionalRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(EventSourcingProperties.class)
public class EventSourcingConfig {

    @Bean
    Clock kernelClock() {
        return Clock.systemUTC();
    }

    @Bean
    EventTypeRegistry eventTypeRegistry() {
        return new EventTypeRegistry();  // domain milestones register their event types here
    }

    @Bean
    UpcasterChain upcasterChain() {
        return new UpcasterChain(List.of());  // upcasters are added when an event revision bumps
    }

    @Bean
    PayloadCodec payloadCodec(ObjectMapper mapper) {
        return new JacksonPayloadCodec(mapper);
    }

    @Bean
    EventSerializer eventSerializer(ObjectMapper mapper, EventTypeRegistry registry,
                                    UpcasterChain upcasters, Clock kernelClock) {
        return new JacksonEventSerializer(mapper, registry, upcasters, kernelClock);
    }

    @Bean
    TransactionalRunner transactionalRunner(PlatformTransactionManager transactionManager) {
        return new SpringTransactionalRunner(new TransactionTemplate(transactionManager));
    }

    @Bean
    EventStore eventStore(JdbcTemplate jdbc, TransactionalRunner tx) {
        return new JdbcEventStore(jdbc, tx);
    }

    @Bean
    SnapshotStore snapshotStore(JdbcTemplate jdbc) {
        return new JdbcSnapshotStore(jdbc);
    }

    @Bean
    TokenStore tokenStore(JdbcTemplate jdbc) {
        return new JdbcTokenStore(jdbc);
    }

    @Bean
    SagaStore sagaStore(JdbcTemplate jdbc) {
        return new JdbcSagaStore(jdbc);
    }

    @Bean(destroyMethod = "close")
    CommandBus commandBus(EventSourcingProperties properties) {
        return new StripedCommandBus(properties.commandStripes());
    }

    @Bean
    DeadlineScheduler deadlineScheduler(JdbcTemplate jdbc, PayloadCodec codec,
                                        EventTypeRegistry registry, Clock kernelClock) {
        return new JdbcDeadlineScheduler(jdbc, codec, registry, kernelClock);
    }
}
```

Append to `src/main/resources/application.yml` (top level):

```yaml
banking:
  eventsourcing:
    command-stripes: 8
    batch-size: 100
    poll-interval: 100ms
    snapshot-threshold: 100
    max-command-attempts: 3
    deadline-poll-interval: 500ms
```

- [ ] **Step 4: Run the test — verify it passes**

Run: `mvn -B test -Dtest=KernelWiringTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/banking/adapter/out/eventstore src/main/resources/application.yml src/test/java/com/example/banking/infra/KernelWiringTest.java
git commit -m "feat: wire event-sourcing kernel beans and configuration properties"
```

---

### Task 11: ArchUnit rules — kernel and domain purity

**Files:**
- Create: `src/test/java/com/example/banking/architecture/EventSourcingKernelRulesTest.java`
- Modify: `pom.xml` — only if `archunit-junit5` is not already present (walking-skeleton Task 6 adds it); if missing, add:

```xml
<dependency>
    <groupId>com.tngtech.archunit</groupId>
    <artifactId>archunit-junit5</artifactId>
    <version>1.4.0</version>
    <scope>test</scope>
</dependency>
```

**Interfaces:**
- Consumes: the package layout produced by Tasks 1–10.
- Produces: enforced dependency rules; later milestones' domain code fails the build if it imports Spring/Jackson/adapter classes.

- [ ] **Step 1: Write the rules (they must pass immediately — the kernel is already pure)**

`src/test/java/com/example/banking/architecture/EventSourcingKernelRulesTest.java`:

```java
package com.example.banking.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * The kernel replaces Axon; unlike Axon annotations, it grants the domain no framework
 * touchpoint. These rules keep both the kernel and the domain free of infrastructure.
 */
@AnalyzeClasses(packages = "com.example.banking", importOptions = ImportOption.DoNotIncludeTests.class)
class EventSourcingKernelRulesTest {

    @ArchTest
    static final ArchRule kernelDependsOnlyOnItselfVavrAndJava =
            classes().that().resideInAPackage("com.example.banking.eventsourcing..")
                    .should().onlyDependOnClassesThat()
                    .resideInAnyPackage("com.example.banking.eventsourcing..", "io.vavr..", "java..");

    @ArchTest
    static final ArchRule domainDependsOnlyOnItselfKernelVavrAndJava =
            classes().that().resideInAPackage("com.example.banking.domain..")
                    .should().onlyDependOnClassesThat()
                    .resideInAnyPackage("com.example.banking.domain..",
                            "com.example.banking.eventsourcing..", "io.vavr..", "java..");
}
```

- [ ] **Step 2: Run the test — verify it passes**

Run: `mvn -B test -Dtest=EventSourcingKernelRulesTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: PASS (2 rules). If the kernel rule fails, the violation list names the offending import —
fix the kernel class (move the infrastructure concern into `adapter.out.eventstore`), never widen
the rule.

- [ ] **Step 3: Prove the rule bites (temporary red)**

Temporarily add `import org.springframework.jdbc.core.JdbcTemplate;` plus a field
`JdbcTemplate probe;` to `src/main/java/com/example/banking/eventsourcing/EventStore.java`, rerun:

Run: `mvn -B test -Dtest=EventSourcingKernelRulesTest > target/mvn-out.txt 2>&1; tail -n 30 target/mvn-out.txt`
Expected: FAIL naming `EventStore` depending on `org.springframework.jdbc`.

Revert the probe edit, rerun, expect PASS again.

- [ ] **Step 4: Run the full suite**

Run: `mvn -B verify > target/mvn-out.txt 2>&1; tail -n 60 target/mvn-out.txt`
Expected: BUILD SUCCESS — all kernel tests plus the pre-existing walking-skeleton tests.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/example/banking/architecture/EventSourcingKernelRulesTest.java
git commit -m "test: enforce kernel and domain purity with ArchUnit rules"
```

---

### Task 12: Documentation swap — remove every stale Axon reference

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md` (supersede banner on Task 5)

No code, no tests; the deliverable is that `grep -ri axon ARCHITECTURE.md CLAUDE.md` returns only
historical/superseded mentions, each explicitly marked as such.

- [ ] **Step 1: Update `ARCHITECTURE.md`**

Apply these replacements (section by section):

1. System-context bullet `- **MySQL** holds both the Axon event store (truth) and the projection tables (read models).` becomes:

```markdown
- **MySQL** holds both the event store (truth) and the projection tables (read models).
```

2. In the "Hexagonal layers" diagram, replace the outbound-adapters line
`outbound adapters  Axon event store (MySQL/JPA), projection repositories (MySQL),` with:

```
outbound adapters  JDBC event store (MySQL), projection repositories (MySQL),
```

3. Replace the whole "Package structure (target)" code block with:

```
com.example.banking
├── eventsourcing           # DIY kernel: ports + pure logic (Vavr only, no infrastructure)
├── domain                  # pure domain — implements kernel interfaces, zero framework imports
│   ├── user                #   User aggregate, events
│   ├── account             #   Account aggregate, events, invariants
│   ├── transfer            #   Transfer saga (pure state machine)
│   └── shared              #   Money, identifiers, DomainError (value objects)
├── application             # command/query gateways, ports (interfaces)
│   ├── command
│   └── query
└── adapter
    ├── in.web              # REST controllers, DTOs, error mapping
    └── out
        ├── eventstore      # JDBC event store, snapshots, tokens, sagas, deadlines, serialization
        ├── projection      # JPA projection entities + repositories, tracking processors
        ├── cache           # Redis
        └── messaging       # Kafka relay processor (spring-kafka)
```

4. Replace the paragraph `Axon annotations (@Aggregate, @CommandHandler, @EventSourcingHandler, saga handlers) are the one permitted framework touchpoint inside domain — idiomatic for Axon. All other infrastructure stays in adapter.` with:

```markdown
The domain has **no framework touchpoint at all**: aggregates implement the kernel's pure
`AggregateBehaviour` (decide/evolve) interface and sagas implement `SagaBehaviour` — plain Java
against a dependency-free kernel package. All infrastructure stays in `adapter`.
```

5. In the write-model sequence diagram, replace `participant GW as Axon CommandGateway` with `participant GW as CommandBus (striped)`.

6. In the read-model section, replace `ES[(Event store)] --> EP[Tracking event processors]` label text and the sentence `Event processors consume events and maintain projections…` — the mechanism sentence becomes:

```markdown
Kernel tracking processors poll the event store with persisted tokens, maintain projections in
MySQL (token + projection updated in one transaction), and can be reset to rebuild a projection
from the start of the stream. The balance projection is cached in Redis.
```

7. In "Event distribution (Kafka)", replace the paragraph with:

```markdown
Domain events are relayed to Kafka by a dedicated kernel tracking processor using `spring-kafka`,
forming the event-driven backbone and the seam for a future real external-bank integration. Kafka
carries events outward; it is explicitly not the event store. Delivery is at-least-once; consumers
deduplicate on `event_id`.
```

8. In "Idempotency and concurrency", replace `**Concurrency** — Axon aggregate optimistic locking (event sequence numbers)…` with:

```markdown
- **Concurrency** — optimistic locking via the event store's unique `(aggregate_id, sequence_nr)`
  key serializes concurrent commands on the same account; the command bus additionally routes
  same-aggregate commands onto one executor stripe. Conflicts are retried (3 attempts); exhausted
  retries surface as `409 Conflict`.
```

9. In "Cross-cutting concerns", replace the Schema bullet with:

```markdown
- **Schema** — Flyway manages all schemas: projections and the kernel tables (event store,
  snapshots, tracking tokens, sagas, deadlines).
```

10. In "Technology mapping", replace the rows:

| Concern | Technology |
| --- | --- |
| ES / CQRS / sagas | DIY event-sourcing kernel (`com.example.banking.eventsourcing`, see the [design spec](docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md)) |
| Event store | MySQL 9.7 (JDBC, Flyway-managed schema) |
| Testing | JUnit 5, kernel GWT fixtures, ArchUnit, Testcontainers, AssertJ |

and delete the trailing compatibility-notes sentence about Axon 4 / `extension-kafka`.

- [ ] **Step 2: Update `CLAUDE.md`**

1. In the architecture table, replace `adapter.out  Axon event store (MySQL/JPA)…` wording with `adapter.out  JDBC event store (MySQL), projection repos (MySQL), Redis, Kafka`.
2. Replace the write-path sentence naming the Axon `CommandGateway` with the kernel `CommandBus`; replace `Axon tracking event processors` with `kernel tracking processors`.
3. Replace the conventions bullet starting `**Axon annotations in domain are allowed**…` with:

```markdown
- **The domain is 100 % pure.** Aggregates and sagas implement the kernel's `AggregateBehaviour`
  / `SagaBehaviour` interfaces (`com.example.banking.eventsourcing` — plain Java + Vavr, no
  framework). ArchUnit enforces: `eventsourcing` depends only on itself + Vavr; `domain` depends
  only on itself + `eventsourcing`; controllers must not reference aggregates directly; value
  objects and events are immutable.
```

4. In "Bleeding-edge stack risks", delete the two Axon bullets (Axon 5 coordinates, Axon Server) and the `extension-kafka` bullet; add:

```markdown
- **Event sourcing / CQRS / sagas are hand-built** — the DIY kernel in
  `com.example.banking.eventsourcing` (design:
  `docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md`). There is no Axon dependency;
  do not reintroduce one.
```

5. Update the "Event store vs bus" paragraph: replace `MySQL (Axon embedded JPA store)` with `MySQL (kernel JDBC store)` and `an Axon event handler relays domain events` with `a kernel tracking processor relays domain events`.

- [ ] **Step 3: Mark walking-skeleton Task 5 superseded**

In `docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md`, directly under the heading
`### Task 5: Axon Framework 5 event store on MySQL (the riskiest wiring)`, insert:

```markdown
> **SUPERSEDED (2026-07-24):** Axon was replaced by the DIY event-sourcing kernel before this task
> was ever built. Do not execute this task. See
> `docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md` and
> `docs/superpowers/plans/2026-07-24-diy-event-sourcing-kernel.md`.
```

- [ ] **Step 4: Verify no live Axon references remain**

Run: `grep -rni axon ARCHITECTURE.md CLAUDE.md docs/superpowers/plans/2026-07-24-diy-event-sourcing-kernel.md`
Expected: matches only in historical context (the superseded banner, the design spec's own
"replaces Axon" framing, this plan's preamble). Any match describing Axon as *current* behavior is
a defect — fix it.

Run the build once more to prove docs-only changes: `mvn -B verify > target/mvn-out.txt 2>&1; tail -n 20 target/mvn-out.txt`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add ARCHITECTURE.md CLAUDE.md docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md
git commit -m "docs: replace Axon references with DIY event-sourcing kernel"
```

---

## Out of scope for this milestone (comes with milestones 1+)

- Concrete aggregates (`User`, `Account`), the real transfer saga, and their event registrations.
- Projections (`account_balance`, `movements`, `transaction_status`), their `TrackingProcessor`
  instances, and the Redis balance cache.
- The Kafka relay processor instance.
- The transaction-status recorder, idempotency guard (Redis `SETNX` + unique constraint), REST
  endpoints, and the 409/RFC 7807 error mapping — the spec's §5.3 write-path behavior needs the
  application layer, which the vertical slices own.
