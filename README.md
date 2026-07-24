# banking-api

A single Spring Boot service simulating core banking (register users, open EUR accounts, run
deposit / withdrawal / transfer transactions, query balance and movements). It is the vehicle for
demonstrating an event-sourced, CQRS, event-driven, hexagonal, DDD design with a functional bias.

- Target architecture: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Functional design and API contract: [`docs/superpowers/specs/2026-07-24-banking-api-design.md`](docs/superpowers/specs/2026-07-24-banking-api-design.md)
- Build plan (Milestone 0, the walking skeleton): [`docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md`](docs/superpowers/plans/2026-07-24-banking-api-walking-skeleton.md)

## Requirements

- JDK 26 (built and tested on Amazon Corretto 26).
- A running Docker (or Podman) daemon — the tests start real MySQL, Redis, and Kafka containers
  via Testcontainers, and `docker compose` is used for local runs.

## Run locally

Start the backing services (MySQL, Redis, Kafka), then run the app:

```
docker compose up -d
mvn spring-boot:run
```

Health check: <http://localhost:8080/actuator/health>

The app reads its connection defaults from `src/main/resources/application.yml` and points at
`localhost` (MySQL `3306`, Redis `6379`, Kafka `9092`) out of the box. Each host is overridable via
an environment variable (`DB_HOST`, `REDIS_HOST`, `KAFKA_HOST`).

Stop the services with:

```
docker compose down
```

The `docker-compose.yml` images are pinned to the same versions the tests use
(`mysql:9.7`, `redis:8.8`, `apache/kafka:3.9.1`) so the local and test environments agree. The
Kafka service is a single-node KRaft broker advertising `localhost:9092`.

## Test

```
mvn verify
```

This needs a running Docker/Podman daemon: the infrastructure tests spin up MySQL, Redis, and Kafka
containers with Testcontainers (they do not use `docker-compose.yml`).

## Current state — Milestone 0 (walking skeleton)

This is a de-risking spike that stands up the empty-but-running stack. What is wired and verified
today:

- Spring Boot 4.1 service boots with an actuator health endpoint (`SmokeTest`).
- MySQL datasource + Flyway migration on a real MySQL container (`MySqlFlywayTest`).
- Redis connectivity on a real Redis container (`RedisTest`).
- Kafka producer/consumer round-trip on a real Kafka container (`KafkaTest`).

Deferred to a later milestone (see the plan and progress ledger for the findings):

- **Axon event store** — Axon Framework 5 has no Spring Boot starter published yet, so the
  event-sourced write side is not wired in this milestone.
- **ArchUnit hexagonal-boundary rule** — ArchUnit cannot yet read Java 26 bytecode, so the
  architecture-enforcement test is deferred.

The event-sourced / CQRS / hexagonal design described in `ARCHITECTURE.md` and the spec is the
target; it is not implemented yet.
