# Core Banking API — Design Specification

- **Date:** 2026-07-24
- **Status:** Approved design, ready for implementation planning
- **Source:** `.claude/first-description.md`

## 1. Objective

Build a REST API that simulates a core banking process. Users register, open accounts, execute
transactions (deposit, withdrawal, transfer), and query their balance and movements. The system is
built to demonstrate a full event-sourced, CQRS, event-driven, hexagonal, domain-driven design with
a functional-programming bias.

## 2. Scope

### In scope

- User self-registration (no authentication).
- Account creation; a user may own many accounts, an account is owned by exactly one user.
- Accounts are Euro-only with a single balance, reconstructed from the event history.
- Transactions: deposit (from external), withdrawal (to external), transfer (internal
  account-to-account). Idempotent, retryable, uniquely identified, not duplicated under concurrency.
- Queries: current balance, last movements (paginated).

### Out of scope (noted for the future)

- Authentication / authorization beyond ownership enforcement.
- Account closure or deletion.
- Multi-currency and FX.
- Real external-bank settlement (the external leg of deposits/withdrawals does not move money).
- Sensitive-data secure storage — encryption at rest, PII protection (user name/email, event payloads), GDPR erasure.

## 3. Technology stack

| Concern | Choice |
| --- | --- |
| Language / runtime | Java 26 |
| Framework | Spring Boot 4.1.0 (Spring Framework 7) |
| Event sourcing / CQRS / sagas | Axon Framework 5 |
| Event store | MySQL 9.7 via Axon `EmbeddedEventStore` + JPA/JDBC storage engine |
| Event distribution bus | Apache Kafka via `spring-kafka` (Axon event-handler relay) |
| Read models / projections | MySQL 9.7 (Spring Data JPA) |
| Cache / idempotency | Redis 8.8 (Spring Data Redis) |
| Schema migrations | Flyway |
| Functional programming | Vavr (`Either`, `Try`, immutable collections) |
| API docs / validation | springdoc-openapi, Jakarta Bean Validation |
| Testing | JUnit 5, Axon test fixtures, ArchUnit, Testcontainers (MySQL/Redis/Kafka), AssertJ |
| Observability | Spring Boot Actuator, Micrometer / OpenTelemetry |

### Compatibility notes / risks

- **Java 26**: Spring Boot 4.1 / Spring Framework 7 officially support up to Java 25 (Java 17
  baseline). Java 26 is one release past the blessed line — expected to run, not officially certified.
  Verify at build time; fall back to Java 25 if issues arise.
- **Axon Framework 4 reached end of life 2026-06-30.** Axon 5 is the required line and supports
  Spring Boot 4. Axon 5 is a new programming model with thinner documentation than v4 — budget
  learning time.
- **`extension-kafka`**: the Axon Kafka extension has **no Axon 5 release** (still on the 4.x line as
  of 2026-07). Kafka integration therefore does **not** use the extension; instead an Axon event
  handler relays domain events to Kafka via plain `spring-kafka`. Kafka remains the broker; only the
  Axon-specific bridge is dropped.
- **Kafka is a distribution bus, not an event store** (AxonIQ's own guidance). MySQL is the source
  of truth for events; Kafka carries them outward to consumers.

## 4. Non-functional requirements — how each is satisfied

- **Event-driven**: domain state changes are events; Axon tracking processors and Kafka consumers
  react to them. Kafka is the event-driven backbone between the write side and downstream consumers.
- **Domain-driven design**: explicit aggregates (User, Account), value objects (Money, identifiers),
  ubiquitous language in events and commands, one bounded context (Core Banking).
- **Hexagonal architecture**: domain at the centre, application ports around it, inbound (REST) and
  outbound (persistence, Redis, Kafka) adapters at the edges. Enforced by ArchUnit.
- **CQRS**: separate write path (commands → aggregates → event store) and read path (projections →
  query endpoints). Different models on each side.
- **Asynchronous processing**: commands dispatched asynchronously; write endpoints return `202
  Accepted` with a transaction reference; projections are eventually consistent.
- **Functional programming**: domain operations return Vavr `Either<DomainError, Result>` rather than
  throwing for business-rule failures; immutable value objects and events; Java 26 records, sealed
  types, and pattern matching for modelling.

## 5. Architecture

Single service, single bounded context. Layered per hexagonal architecture:

```
inbound adapter    REST controllers (Spring MVC), RFC 7807 problem+json error mapping
application        command gateway, query services, transaction-status service (ports)
domain             aggregates, value objects, domain events, invariants
outbound adapter   Axon event store (MySQL/JPA), projection repositories (MySQL),
                   Redis cache, Kafka event publisher
```

**Axon-annotation placement**: Axon annotations (`@Aggregate`, `@CommandHandler`,
`@EventSourcingHandler`, saga handlers) live on the aggregates and sagas in the domain layer, which is
the idiomatic Axon approach. All infrastructure concerns (JPA, Redis, Kafka, web) remain in adapters.

**ArchUnit rules** (enforced as tests):

- Domain must not depend on web, persistence, Redis, or Kafka packages.
- Controllers must not reference aggregates directly — only application gateways/services.
- Value objects and events are immutable.

## 6. Domain model

### Aggregates

- **User** (event-sourced) — identity and registration. Events: `UserRegistered`. Holds account
  ownership by reference only.
- **Account** (event-sourced) — owner `UserId`, balance folded from events, currency fixed to EUR,
  status. Events: `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`, `AccountDebited`,
  `AccountCredited`. Enforces the **no-overdraft** invariant on withdrawal and debit.

### Value objects

- **Money** — integer minor units (cents) stored as `long`, currency EUR. No floating point. API
  exposes a decimal string; internal maths is integer.
- **UserId, AccountId, TransactionId** — typed identifiers.
- **ExternalAccountRef** — opaque IBAN-like string; the external counterparty of a deposit or
  withdrawal. No money moves on the external side.

## 7. Transaction model

- **Deposit** — single command on one Account; credits from an external reference.
- **Withdrawal** — single command on one Account; debits to an external reference; rejected if it
  would breach the no-overdraft invariant.
- **Transfer** — spans two Account aggregates, coordinated by an **Axon Saga** with compensation:
  1. `TransferRequested` starts the saga; it issues `DebitAccount` to the source.
  2. `AccountDebited` → saga issues `CreditAccount` to the target.
  3. `AccountCredited` → saga emits `TransferCompleted`.
  4. Debit fails (insufficient funds) → `TransferRejected`. Credit fails after a successful debit →
     compensating `RefundAccount` on the source → `TransferFailed`.

A transfer is deliberately **not** a single database transaction: two aggregates plus asynchronous
processing require a saga/process-manager, per DDD's one-aggregate-per-transaction rule.

Additional rules: amount must be strictly positive; a transfer with equal source and target is
rejected; opening an account requires an existing user.

## 8. CQRS flow

### Write path

1. REST controller validates input and reads `X-User-Id`.
2. Controller maps the request to a command and dispatches it asynchronously via the Axon
   `CommandGateway`.
3. The target aggregate is rehydrated by replaying its events from the MySQL event store.
4. The command handler validates invariants (Vavr `Either`), then applies a domain event.
5. The event is appended to the event store and published.
6. The endpoint returns `202 Accepted` with `transactionId` and a `statusUrl`.

### Read path

- Axon tracking event processors consume events and build projections in MySQL:
  `account_balance`, `movements`, `transaction_status`.
- The balance projection is cached in Redis for fast reads.
- Query endpoints read exclusively from projections (never from the event store directly).

### Distribution

- An Axon event handler relays domain events to Kafka topic(s) via `spring-kafka`, forming the
  event-driven backbone and the seam for any future external-bank integration. (The Axon
  `extension-kafka` has no Axon 5 release, so `spring-kafka` is used directly.)

## 9. Idempotency and concurrency

- The client supplies an `Idempotency-Key` that is used as the `TransactionId`. Re-submitting the same
  key returns the existing transaction's status and never produces a second movement. A processed-key
  set is kept in Redis (with TTL) and backed by a unique constraint in the `transaction_status`
  projection.
- Concurrent commands on the same account are serialized by Axon's aggregate optimistic locking
  (event sequence numbers). On conflict the command is retried; if retries are exhausted the API
  returns `409 Conflict`.

## 10. API contract (draft)

Caller identity is passed in the `X-User-Id` header. There is no authentication; the system enforces
that the account belongs to the caller (wrong owner → `403`).

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| POST | `/users` | `{ }` (or profile fields) | `201 { userId }` |
| POST | `/accounts` | `{ userId }` | `202 { accountId, statusUrl }` |
| POST | `/accounts/{accountId}/transactions` | `{ type, amount, idempotencyKey, counterparty }` | `202 { transactionId, status: PENDING, statusUrl }` |
| GET | `/transactions/{transactionId}` | — | `200 { transactionId, status: PENDING\|COMPLETED\|REJECTED\|FAILED, reason? }` |
| GET | `/accounts/{accountId}/balance` | — | `200 { balance, currency: EUR, asOf }` |
| GET | `/accounts/{accountId}/movements` | `?cursor=&limit=` | `200 { items[], nextCursor }` |

- `type` is one of `deposit`, `withdrawal`, `transfer`.
- `counterparty` is an `ExternalAccountRef` for deposit/withdrawal, or a target `accountId` for
  transfer.
- Movements pagination is cursor-based; default `limit` 20, maximum 100.

## 11. Error handling

- Business-rule outcomes are modelled as Vavr `Either<DomainError, Result>` and mapped to RFC 7807
  `application/problem+json`.
- HTTP mapping: not owner → `403`; resource not found → `404`; insufficient funds, self-transfer,
  non-positive amount → `422`; concurrency conflict after retries → `409`.
- Because transfers are asynchronous, a transfer failure surfaces as a transaction **status**
  (`REJECTED` / `FAILED`) with a reason, not as an HTTP error on the original `202` request.
- Idempotent replays return the original transaction result rather than an error.

## 12. Data model (high level)

- **Event store** (MySQL, managed by Axon): domain-event and snapshot tables. Source of truth.
- **Projections** (MySQL): `account_balance` (accountId, ownerId, balance, version, updatedAt),
  `movements` (append-only: accountId, transactionId, type, amount, direction, timestamp, sequence),
  `transaction_status` (transactionId, type, status, reason, accountId, createdAt, updatedAt).
- **Redis**: cached balance per account; idempotency-key set with TTL.
- Flyway manages projection schemas; the Axon event-store schema is created via Axon/Hibernate.

## 13. Testing strategy

- **Domain / aggregate**: Axon `AggregateTestFixture` (given prior events → when command → expect
  events or rejection). Cover the no-overdraft invariant, positive-amount and self-transfer rules.
- **Saga**: Axon `SagaTestFixture` for transfer happy path, debit-rejection, and credit-failure
  compensation.
- **Functional**: assert both arms of every `Either` returned by domain operations.
- **Architecture**: ArchUnit boundary and immutability rules.
- **Integration**: Testcontainers for MySQL, Redis, and Kafka — projection updates, balance caching,
  Kafka publication, and REST endpoints (MockMvc / RestAssured).
- **Idempotency & concurrency**: duplicate-key submissions return one movement; parallel same-account
  commands do not double-apply.

## 14. Gap and inconsistency analysis

Issues found in the original description and the resolution taken into this design:

1. **"No login" vs "cannot use someone else's account"** — resolved as ownership enforcement, not
   authentication. Caller identity arrives in `X-User-Id`; the system verifies account ownership but
   does not prove the caller's identity.
2. **"Connect to external banks" (objective) vs external money movement out of scope** — external
   accounts are opaque references only; deposits/withdrawals name an external counterparty but move no
   external money. Kafka is the reserved integration seam for a future real connection.
3. **"Balance can be reconstructed from movements"** — interpreted as full event sourcing; the event
   store is the source of truth and balance is a fold/projection.
4. **Transfer atomicity across two accounts** — a transfer touches two aggregates, so it is a saga with
   compensation, not a single database transaction.
5. **Overdraft** — not specified; resolved as **rejected** (no negative balance).
6. **Money representation** — not specified; resolved as integer minor units (cents), EUR fixed, to
   avoid floating-point error.
7. **"Last movements"** — unbounded in the description; resolved as cursor-based pagination
   (default 20, max 100).
8. **Asynchronous semantics** — write endpoints return `202` with a status resource; projections are
   eventually consistent. Clients poll transaction status or query movements/balance.
9. **Unstated rules made explicit** — amount strictly positive; self-transfer rejected; opening an
   account requires an existing user.
10. **Explicitly out of scope** — account closure/deletion, multi-currency, authentication, and real
    external settlement.

## 15. Open items to confirm during implementation planning

- Whether the `User` aggregate carries profile fields or is identity-only.
- Snapshotting threshold for the `Account` aggregate (Axon snapshots to bound replay cost).
- Kafka topic layout (single topic vs per-aggregate) and partitioning key (accountId for ordering).
- Retry/backoff policy for command conflicts and saga compensation.
