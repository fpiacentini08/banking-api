# DIY Event-Sourcing / CQRS Kernel — Design

**Date:** 2026-07-24
**Status:** Approved (supersedes the Axon Framework 5 decision in
[`2026-07-24-banking-api-design.md`](2026-07-24-banking-api-design.md) §Technology mapping and the
walking-skeleton plan's Task 5)

## 1. Context and goal

The approved design mapped event sourcing, CQRS, and sagas to Axon Framework 5. Axon 5 is
preview-stage, its Kafka extension does not exist, and the walking skeleton flagged it as the
riskiest wiring. Before any Axon code was written (Task 5 was never built; tasks 1–4 are merged,
tasks 6–7 are in flight), the decision was taken to **replace Axon with a hand-built ("DIY")
event-sourcing/CQRS kernel in plain Java**.

Goals:

- Full functional parity with what Axon would have provided: event store, aggregate replay,
  optimistic locking, async command dispatch, tracking processors, projection rebuild,
  snapshotting, saga with compensation, saga deadlines, and given/when/then test fixtures.
- Keep the rest of the stack unchanged: Spring Boot 4.1, Spring Data JPA, `spring-kafka`, Redis,
  Vavr, Flyway, Testcontainers.
- Strengthen the architecture: the domain loses its one framework concession (Axon annotations)
  and becomes 100 % pure.

Non-goals (out of scope, deliberately):

- Distributed command bus or multi-node competing processors — single-instance service.
- Event encryption, GDPR erasure, multi-tenancy.
- Generic reusable open-source library polish — the kernel serves this service only, though it is
  written domain-agnostic.

## 2. Decisions summary

| Question | Decision |
| --- | --- |
| Stack scope | Replace Axon only; keep Spring Boot/JPA/Kafka/Redis/Vavr/Flyway |
| Completeness | Full Axon parity: snapshots, resumable processors, rebuild, GWT fixtures, saga deadlines |
| Programming model | Functional `decide`/`evolve` — pure functions, no annotations, no reflection |
| Projection feed | Processors poll the MySQL event store (tokens); Kafka stays distribution-only |
| Sequencing | Tasks 6–7 finish as-is (Axon-free); kernel becomes its own milestone |
| Architecture rules | DIY `ArchCheck` on the JDK ClassFile API (`java.lang.classfile`) — ArchUnit dropped, incompatible with the Java version in use |

## 3. Module structure and dependency rules

New top-level package `com.example.banking.eventsourcing` — the kernel. Pure Java + Vavr only:
no Spring, no JPA, no Jackson types in its API surface.

```
com.example.banking
├── eventsourcing              # DIY kernel — ports + pure logic, zero infrastructure
│   ├── AggregateBehaviour<S,C,E>   # initial(), evolve(S,E), decide(S,C): Either<DomainError,List<E>>
│   ├── EventStore (port)           # append(streamId, expectedVersion, events), readStream, readAllAfter(pos)
│   ├── EventSourcingRepository     # snapshot + replay + decide + append + conflict retry
│   ├── CommandBus (port) + CommandHandler registry
│   ├── TrackingProcessor, TokenStore (port)
│   ├── SnapshotStore (port), Snapshotter
│   ├── SagaManager, SagaStore (port), DeadlineScheduler (port)
│   └── (src/test) AggregateTestFixture, SagaTestFixture
├── domain                     # 100 % pure — implements AggregateBehaviour; no annotations
├── application                # command gateway, query services, transaction-status service
└── adapter
    ├── in.web                 # REST controllers, DTOs, RFC 7807 mapping
    └── out
        ├── eventstore         # JDBC EventStore + SnapshotStore + TokenStore + SagaStore + upcasters
        ├── projection         # projection handlers plugged into TrackingProcessors
        ├── cache              # Redis
        └── messaging          # Kafka relay processor (spring-kafka)
```

Dependency rules (enforced by the DIY `ArchCheck` architecture test — §9):

- `eventsourcing` depends only on itself and Vavr.
- `domain` depends only on itself and `eventsourcing`.
- `adapter.in.web` never references aggregates or the kernel's repository directly — only
  application gateways/services.
- Value objects, events, commands, and aggregate state are immutable (records).

This replaces the previous rule "Axon annotations are the one permitted framework touchpoint in
`domain`" — there is no framework touchpoint any more.

## 4. Event store (MySQL, Flyway-managed)

```sql
CREATE TABLE event_store_sequence (
  id            TINYINT PRIMARY KEY,   -- single row, id = 1
  next_position BIGINT NOT NULL
);

CREATE TABLE domain_event (
  global_position BIGINT       PRIMARY KEY,   -- NOT auto_increment; assigned from sequence row
  aggregate_type  VARCHAR(64)  NOT NULL,
  aggregate_id    CHAR(36)     NOT NULL,
  sequence_nr     BIGINT       NOT NULL,      -- per-stream, 0-based
  event_id        CHAR(36)     NOT NULL,
  event_type      VARCHAR(128) NOT NULL,      -- logical name, decoupled from class name
  revision        INT          NOT NULL,      -- event schema version, drives upcasting
  payload         JSON         NOT NULL,
  metadata        JSON         NOT NULL,      -- correlationId, causationId, userId
  occurred_at     TIMESTAMP(6) NOT NULL,
  UNIQUE KEY uq_stream (aggregate_id, sequence_nr)
);

CREATE TABLE snapshot (
  aggregate_id CHAR(36) PRIMARY KEY,          -- latest-only, overwritten in place
  sequence_nr  BIGINT   NOT NULL,
  revision     INT      NOT NULL,
  payload      JSON     NOT NULL
);
```

Semantics:

- **Optimistic locking.** `append(aggregateId, expectedVersion, events)` inserts rows with
  `sequence_nr = expectedVersion + 1 …`. A concurrent writer violates `uq_stream`; the duplicate-key
  error is mapped to a typed `ConcurrencyConflict` and retried by the repository.
- **Gap-free global ordering.** `global_position` is assigned by reading and incrementing
  `event_store_sequence` under `SELECT … FOR UPDATE` **in the same transaction** as the event
  insert. Commit order therefore equals position order and a rollback leaves no hole. Pollers read
  `WHERE global_position > :token ORDER BY global_position` with no gap-awareness logic.
  Trade-off, accepted deliberately: all appends serialize on that row lock. For a single-instance
  demo-scale service this is well inside tolerance and vastly simpler than Axon's gap-aware tokens.
- **Serialization.** Jackson JSON, applied in `adapter.out.eventstore` only; the kernel API deals
  in a `SerializedEvent` record (type, revision, payload string, metadata string). `event_type` is a
  registered logical name, so class renames never break the store.
- **Upcasting.** An upcaster is a pure function `JsonNode → JsonNode` registered for
  `(event_type, fromRevision)`. On read, the chain runs until the payload reaches the current
  revision, then deserializes. New events are always written at the current revision.
- **Snapshots.** Latest-only, overwritten in place. Loading = read snapshot (if any), replay only
  events with `sequence_nr` greater than the snapshot's. Snapshot payloads carry their own
  `revision`; an incompatible state-shape change bumps the revision and stale snapshots are simply
  ignored and rewritten after the next load (replay from zero remains the fallback truth).

## 5. Write path

### 5.1 Command bus

- `CommandBus.dispatch(Command): CompletableFuture<CommandResult>` where
  `CommandResult = Either<DomainError, CommittedEvents>`.
- Handlers are registered explicitly at startup (`Class<C> → CommandHandler<C>`); no classpath
  scanning, no reflection.
- **Striped execution:** commands are routed to `hash(aggregateId) % N` single-thread executors.
  Commands for the same aggregate execute in submission order (eliminating most optimistic-lock
  conflicts); different aggregates run in parallel. The bus is asynchronous by construction — the
  controller dispatches and immediately returns `202 Accepted { transactionId, status: PENDING,
  statusUrl }` without waiting on the future.

### 5.2 Repository algorithm (inside a stripe)

1. **Load:** latest snapshot (if any) → `readStream(id, afterSequence)` → fold `evolve` from
   `initial()` or the snapshot state. A missing stream for a non-creating command yields
   `AggregateNotFound` (a `DomainError`, → REJECTED).
2. **Decide:** `decide(state, command)` → `Either<DomainError, List<Event>>`. Pure; no I/O.
3. **Append:** on `Right`, `append(id, expectedVersion, events)`. On `ConcurrencyConflict`,
   reload and re-decide, at most 3 attempts; exhaustion surfaces as **HTTP 409 Conflict** — the
   only synchronous business failure mapped to an HTTP error, per the approved design.
4. **Snapshot:** post-commit, if the stream crossed the threshold (every 100 events), write a
   snapshot outside the command's critical path.

### 5.3 Transaction status and idempotency

`decide` returning `Left` produces no event, yet the status endpoint must expose the rejection.
The `transaction_status` table therefore has **two writers with monotonic transitions**
(`PENDING → COMPLETED | REJECTED | FAILED`; terminal states never regress):

- **On accept** (controller): insert `PENDING` row (unique key `transaction_id`).
- **On `Left`** (command-result recorder subscribed to the bus): update to `REJECTED` + reason.
- **On success events** (projection processor): update to `COMPLETED` for single-account
  operations; transfers stay `PENDING` until the saga's terminal event (`TransferCompleted`,
  `TransferRejected`, `TransferFailed`) is projected.

Idempotency: the client `Idempotency-Key` *is* the `TransactionId`. Redis `SETNX` with TTL is the
fast-path guard; the unique constraint on `transaction_status.transaction_id` is the truth. A
replayed key returns the existing status row and dispatches nothing — no second movement.

## 6. Read path — tracking processors

- A `TrackingProcessor` is a named poll loop: read a batch
  (`global_position > token ORDER BY global_position LIMIT :batch`), dispatch each event to its
  registered handlers, advance the token. For MySQL-backed projections, **projection writes and
  the token update commit in one DB transaction** — exactly-once effect per projection. Processors
  with external effects (Redis put, Kafka publish, saga command dispatch) are at-least-once by
  nature; each is made safe idempotently (see §6 Redis, §7, §8).
- Token store: `tracking_token (processor_name PK, position, updated_at)`.
- Poll interval ~100 ms; re-poll immediately while a batch comes back full (catch-up mode).
- Processors (independent — one slow or failing processor never blocks the others):
  `balance-projection`, `movements-projection`, `transaction-status-projection`,
  `saga-transfer`, `kafka-relay`.
- **Rebuild:** per-processor reset = delete its token + truncate its projection tables; on
  restart it replays from position 0. Exposed as an admin operation.
- **Redis balance cache:** the balance processor updates the MySQL projection in-transaction,
  then puts the new balance to Redis post-commit (at-least-once; a lost put self-heals on the
  next event or on cache-miss fallback to the MySQL projection).

## 7. Kafka relay

The relay is just another tracking processor: it reads committed events from the store and
publishes them to Kafka via `spring-kafka`, advancing its token only after broker
acknowledgement. Delivery is at-least-once; downstream consumers deduplicate on `event_id`.
Kafka remains a distribution bus only — never a source of truth, never load-bearing for
projections or sagas.

## 8. Transfer saga and deadlines

- **Persistence:** `saga_instance (saga_id PK, saga_type, state JSON, status)` and
  `saga_association (saga_type, association_key, saga_id)` — events are correlated to instances
  by `transactionId`.
- **Model:** the saga itself is a pure state machine in `domain.transfer`:
  `react(state, event) → (newState, commandsToDispatch, deadlinesToSchedule/Cancel)`. The
  `SagaManager` (kernel) runs on the `saga-transfer` processor: loads/creates the instance,
  calls `react`, persists the new state, dispatches follow-up commands through the CommandBus,
  and schedules/cancels deadlines.
- **Flow** (per the approved design): `TransferRequested` → dispatch `DebitAccount` →
  `AccountDebited` → dispatch `CreditAccount` → `AccountCredited` → `TransferCompleted`.
  Debit rejected → `TransferRejected`. Credit fails after a successful debit → dispatch
  `RefundAccount` (compensation) → `TransferFailed`.
- **Deadlines:** `deadline (deadline_id PK, saga_id, due_at, payload JSON)`. A scheduler polls
  due rows and delivers them to the saga as timeout events (e.g. credit confirmation never
  arrives → compensate). Deadlines are cancelled when the saga reaches a terminal state.
- **Redelivery safety:** a crash between command dispatch and the token commit redelivers the
  event, so saga command dispatch is at-least-once. This is safe because saga commands carry the
  deterministic `transactionId`: the Account aggregate's `decide` rejects a debit/credit whose
  `transactionId` it has already applied, producing no second movement.

## 9. Testing strategy

- **`AggregateTestFixture<S,C,E>`** — pure, no infrastructure:
  `given(events…).when(command).expectEvents(…)` / `.expectError(domainError)`. Both `Either`
  arms are asserted in every aggregate test, per project convention.
- **`SagaTestFixture`** — given a sequence of events, assert dispatched commands and
  scheduled/cancelled deadlines against a recording stub CommandBus.
- **Integration (Testcontainers MySQL):** append/replay round-trip; optimistic-conflict retry;
  processor catch-up after restart; projection rebuild; snapshot write + load; upcaster applied
  on read.
- **Architecture rules — DIY `ArchCheck` (no ArchUnit):** ArchUnit is not compatible with the
  Java version in use, so architecture rules are enforced by a small hand-built checker on the
  **JDK ClassFile API** (`java.lang.classfile`, standard since Java 24 — always understands the
  bytecode the JDK itself produced). `ArchCheck` scans `target/classes`, extracts every referenced
  class per class (constant-pool `ClassEntry`s plus field/method descriptors — ArchUnit-grade
  precision, method bodies included), and evaluates package allow-list rules: the dependency rules
  of §3 plus the hexagonal-boundary rules. A self-test asserts the scanner sees a known dependency
  (adapter → Spring), guarding against a silently empty scan.

## 10. Documentation and plan impact

- `ARCHITECTURE.md` — replace every Axon reference: package tree, write/read sequence diagrams,
  technology-mapping row (`ES / CQRS / sagas → DIY eventsourcing kernel (this spec)`), the
  concurrency note, and the schema note (Flyway now owns the event-store schema too). Replace
  every ArchUnit reference (rules section heading, testing row) with the DIY `ArchCheck` checker.
- `CLAUDE.md` — rewrite the Axon bullets: the annotation concession becomes "domain is 100 %
  pure"; drop the Axon-5-coordinates spike, `axon.axonserver.enabled=false`, and Axon-version
  risk notes. Replace ArchUnit mentions with `ArchCheck` and drop `archunit-junit5` from the
  community-version risk list (the checker has no third-party dependency).
- Walking-skeleton plan — Task 5 (Axon event store) is **superseded by this spec**; the kernel
  is built as its own milestone, planned task-by-task via the writing-plans process after this
  spec is approved.
- These edits land after tasks 6–7 merge (in flight in another session at the time of writing),
  to avoid touching files that session owns.
