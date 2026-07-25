# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state — read this first

The walking skeleton (Milestone 0) is built and green, and the **DIY event-sourcing kernel** that
replaces Axon Framework is implemented in `com.example.banking.eventsourcing` (+ jOOQ adapters in
`adapter.out.eventstore`). The build runs on Java 26 with Spring Boot 4.1. Read the design before
extending it.

- `docs/superpowers/specs/2026-07-24-banking-api-design.md` — the approved functional design, scope,
  API contract, and the gap analysis that resolved ambiguities in the original brief.
- `docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md` — the event-sourcing/CQRS/saga
  kernel design that **supersedes the Axon decision**. This is the source of truth for the ES stack.
- `ARCHITECTURE.md` — the target architecture (layers, package structure, diagrams, tech mapping).
- `docs/superpowers/plans/2026-07-24-diy-event-sourcing-kernel.md` — the task-by-task TDD plan the
  kernel was built from.
- `docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md` — Milestone 0 (built); its
  Task 5 (Axon) is superseded, do not execute it.

The design lists milestones 1–7 (each a vertical slice). Each later milestone's plan is written
*after* the previous one is built and reviewed, so plans stay grounded in code already seen to
compile. Do not plan a later milestone until its predecessor is green.

## What this is

A single Spring Boot service, one bounded context (**Core Banking**), that simulates core banking:
users register (no auth), open EUR-only accounts, run transactions (deposit / withdrawal / transfer),
and query balance and movements. It is deliberately built to demonstrate a full **event-sourced,
CQRS, event-driven, hexagonal, DDD** design with a functional-programming bias.

## Commands

These are the commands the walking-skeleton plan establishes; they apply once `pom.xml` exists.

- **Full build + all tests:** `mvn -B verify`
- **Run one test class:** `mvn -B test -Dtest=SmokeTest`
- **Run the app locally:** `docker compose up -d` then `mvn spring-boot:run` (health at
  `http://localhost:8080/actuator/health`)

**Maven output convention (required):** redirect Maven output to a file under `target/`, then inspect
its tail — do not let long Maven logs flood the transcript:

```
mvn -B <goals> > target/mvn-out.txt 2>&1; tail -n 60 target/mvn-out.txt
```

**Testcontainers:** the infra/integration tests start MySQL, Redis, and Kafka containers, so a running
Docker/Podman daemon is required to run `mvn verify`.

## Architecture (the big picture)

Dependencies point inward. `domain` depends on nothing outside itself; `application` defines ports;
`adapter` implements/drives them.

```
adapter.in.web   REST controllers (Spring MVC), DTOs, RFC 7807 problem+json mapping   (drives)
application      command gateway, query services, transaction-status service = ports  (uses)
domain           aggregates, value objects, domain events, invariants                 (centre)
eventsourcing    DIY ES/CQRS kernel: ports + pure logic (Vavr only, no infrastructure)
adapter.out      jOOQ event store (MySQL), projection repos (MySQL), Redis, Kafka      (implements)
```

Base Java package: `com.example.banking`. Target package tree is in `ARCHITECTURE.md` §"Package
structure".

**Write path (CQRS command side):** controller maps request to a command and dispatches it
**asynchronously** via the kernel `CommandBus` (striped by aggregate id); the endpoint returns
**`202 Accepted`** with a `transactionId` and `statusUrl`. The aggregate is rehydrated by replaying
its events from the MySQL event store (`EventSourcingRepository`), validates invariants via its pure
`decide`, and applies a new domain event that is appended (optimistic locking) and published.

**Read path (query side):** kernel tracking processors poll the event store with persisted tokens
and build MySQL projections (`account_balance`, `movements`, `transaction_status`); the balance
projection is cached in Redis. Query endpoints read **only** from projections — never from the event
store.

**Event store vs bus:** MySQL (kernel jOOQ store) is the **source of truth** for events.
Kafka is a **distribution bus only**, not an event store — a kernel tracking processor relays domain
events to Kafka via plain `spring-kafka`. Balance is a fold over an account's events, reconstructed by
replay.

**Aggregates:** `User` (events: `UserRegistered`) and `Account` (events: `AccountOpened`,
`MoneyDeposited`, `MoneyWithdrawn`, `AccountDebited`, `AccountCredited`; invariant: no overdraft,
EUR only). A **transfer spans two Account aggregates**, so it is a kernel **saga with compensation**
(debit → credit → complete; `RefundAccount` if credit fails) — deliberately *not* one DB transaction,
per DDD's one-aggregate-per-transaction rule.

## Conventions and constraints that are easy to get wrong

- **The domain is 100 % pure.** Aggregates and sagas implement the kernel's `AggregateBehaviour`
  (pure `decide`/`evolve`) and `SagaBehaviour` interfaces (`com.example.banking.eventsourcing` —
  plain Java + Vavr, no annotations, no reflection, no framework). *All* infrastructure (jOOQ, Redis,
  Kafka, web, Jackson) stays in `adapter`. The DIY `ArchCheck` test (JDK ClassFile API, **no
  ArchUnit**) enforces: `eventsourcing` depends only on itself + Vavr + `java.`; `domain` depends
  only on itself + `eventsourcing` + Vavr + `java.`; value objects and events are immutable.
- **Database access is jOOQ-only, in a Repository.** All SQL goes through the jOOQ `DSLContext` inside a dedicated `adapter.out.*` Repository class; projections/services/controllers never touch the DB directly — they depend on a Repository. jOOQ is **DSL-only** (string-based `DSL.table`/`DSL.field`, no code generation). Flyway still owns all DDL; jOOQ is DML only. The kernel (`eventsourcing`) stays jOOQ-free.
- **Money is never floating point.** Integer minor units (cents) as `long`, currency fixed to EUR.
  The API exposes a decimal string; internal maths is integer.
- **Functional error handling.** Business-rule outcomes are Vavr `Either<DomainError, Result>`, not
  thrown exceptions, mapped to RFC 7807 `application/problem+json`. Async transfer failures surface
  as a transaction **status** (`REJECTED` / `FAILED`) with a reason — not as an HTTP error on the
  original `202`. Assert **both arms** of every `Either` in tests.
- **Idempotency:** the client `Idempotency-Key` *is* the `TransactionId`; replays return the original
  status and produce no second movement. Tracked in Redis (TTL) + unique constraint on
  `transaction_status`.
- **Concurrency:** optimistic locking via the event store's unique `(aggregate_id, sequence_nr)`
  key serializes concurrent same-account commands (the command bus also routes them onto one
  executor stripe); exhausted retries surface as `409 Conflict`.
- **Ownership, not auth:** caller identity arrives in `X-User-Id`; there is no login. Wrong owner is
  `403`.
- **Sensitive data is plaintext (out of scope for now).** Name, email, and event payloads are not encrypted/masked; secure storage is deferred to a future milestone.

## Bleeding-edge stack risks (decided at build time, not guessed)

The stack is intentionally past the certified line; resolve these at build time, not from memory:

- **Java 26** — builds and tests green on Corretto 26 with Spring Boot 4.1 / Spring Framework 7
  (certified only to Java 25, but works). Requires a Java 26 JDK on `JAVA_HOME`.
- **Event sourcing / CQRS / sagas are hand-built** — the DIY kernel in
  `com.example.banking.eventsourcing` (design:
  `docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md`). There is **no Axon dependency**;
  do not reintroduce one.
- **Architecture rules use a DIY `ArchCheck`** on the JDK ClassFile API (`java.lang.classfile`), not
  ArchUnit (incompatible with the Java version in use). No third-party arch-test dependency.
- **Jackson 3** (`tools.jackson`, the Spring Boot 4.1 default) — Jackson serialization lives only in
  `adapter.out.eventstore`; its exceptions are unchecked.
- **Kafka uses `spring-kafka` directly** as a distribution bus.
- **jOOQ is DSL-only** — jOOQ 3.21.5 (Boot-managed by `spring-boot-starter-jooq`) resolves and runs on
  Java 26 with dialect `MYSQL` confirmed effective at runtime. **No code generation:** a build-time
  codegen tool on Java 26 is the same risk class that broke ArchUnit and Axon, so all SQL is written
  against the string-based DSL (`DSL.table` / `DSL.field`) and Flyway keeps owning DDL.
- Community/tool versions (`testcontainers-redis`, Vavr) may be stale — resolve current versions if so.

When a bleeding-edge deviation is made (library version, API rename), **write it into the commit
history** so the next milestone's planning starts from proven facts.

For Spring Boot class/config/behaviour questions, use Context7 (`spring-boot`, `spring-framework`,
`spring-cloud`, `spring-security`, `spring-data`) rather than reverse-engineering from JARs.

## Git

- Conventional Commits. **No tool-attribution footer** (no "Generated by Claude Code" or similar).
- This repo uses the **personal** GitHub account `fpiacentini08`, not a company account.
