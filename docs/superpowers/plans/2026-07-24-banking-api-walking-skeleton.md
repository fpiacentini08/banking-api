# Banking API — Walking Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up an empty-but-running Spring Boot service that boots the full bleeding-edge stack — Java 26, Spring Boot 4.1, Axon Framework 5 (MySQL event store), Redis, and Kafka — green, with an enforced hexagonal package boundary. No banking behaviour yet.

**Architecture:** This is milestone 0, a deliberate de-risking spike. Its only job is to prove the risky pieces boot and connect *together* before any domain logic is written. Every later milestone is a vertical slice (one aggregate/flow end to end) planned against the stack this milestone proves compiles.

**Tech Stack:** Java 26, Spring Boot 4.1.0, Axon Framework 5, MySQL 9.7, Redis 8.8, Apache Kafka (via `spring-kafka`), Flyway, Vavr, ArchUnit, Testcontainers, JUnit 5, AssertJ, Maven.

## Global Constraints

- Java 26. Spring Boot 4.1 / Spring Framework 7 are officially certified only to Java 25; Java 26 is one release past the blessed line. If the build or context load fails specifically on Java 26, fall back to Java 25 and record it — this milestone is where that gets decided.
- Spring Boot parent version: `4.1.0` exactly.
- Axon Framework 5 is at preview/early-release stage; the artifact version and groupId may differ from any value written here. Every Axon coordinate MUST be resolved against Maven Central at build time (see Task 5), not copied verbatim from this plan.
- Kafka integration uses `spring-kafka` (Spring Boot managed version), NOT the Axon `extension-kafka` (no Axon 5 release exists).
- Axon Server is not used. Disable it with `axon.axonserver.enabled=false`; the embedded JPA event store on MySQL is the source of truth.
- Money is never floating point (no domain money in this milestone, but the constraint stands for all later milestones): integer minor units (cents), EUR only.
- Base Java package: `com.example.banking`.
- Commit style: Conventional Commits, no tool-attribution footer.

## Milestone roadmap (for visibility — only Milestone 0 is detailed below)

Each milestone is its own plan, written *after* the previous one is built and reviewed, so later plans are grounded in code already seen to compile.

| # | Milestone | Deliverable (working, testable) |
| --- | --- | --- |
| **0** | **Walking skeleton (THIS PLAN)** | Service boots; MySQL + Redis + Kafka + Axon 5 event store all connect green; ArchUnit boundary rule runs and can fail. |
| 1 | User registration | First Axon aggregate end to end: `POST /users` → `UserRegistered` in event store → users projection. Proves the command→event→projection loop. |
| 2 | Account opening | `Account` aggregate; `POST /accounts` (owner must exist); `account_balance` projection; `GET /accounts/{id}/balance` returns 0. |
| 3 | Deposit | Single-account transaction; `202` + async status; `transaction_status` + `movements` projections; idempotency (Redis + unique constraint); `GET /transactions/{id}`, `GET /accounts/{id}/movements`. |
| 4 | Withdrawal + no-overdraft | `MoneyWithdrawn`; insufficient-funds rejection surfaced as transaction status. |
| 5 | Transfer saga | Axon saga across two accounts; debit → credit → complete; compensation (`RefundAccount`) on credit failure; self-transfer rejected. |
| 6 | Kafka event distribution | Axon event handler relays domain events to Kafka via `spring-kafka`; consumer/integration test asserts publication. |
| 7 | Concurrency + cache + hardening | Optimistic-locking conflict → `409`; Redis balance cache; Account snapshotting; OpenAPI polish; observability. |

---

## File structure (this milestone)

- Create: `pom.xml` — Maven build, dependency management.
- Create: `src/main/java/com/example/banking/BankingApiApplication.java` — Spring Boot entry point.
- Create: `src/main/resources/application.yml` — datasource, redis, kafka, axon, actuator config.
- Create: `src/main/resources/db/migration/V1__baseline.sql` — Flyway baseline.
- Create: `src/main/java/com/example/banking/domain/package-info.java` — marker for the domain package.
- Create: `src/main/java/com/example/banking/domain/Marker.java` — one real domain class so ArchUnit has something to evaluate.
- Create: `src/main/java/com/example/banking/adapter/in/web/HealthNote.java` — one real adapter class for the same reason.
- Create: `docker-compose.yml` — MySQL, Redis, Kafka for local run.
- Create: `README.md` — how to build, test, and run.
- Test: `src/test/java/com/example/banking/SmokeTest.java` — context loads + health.
- Test: `src/test/java/com/example/banking/infra/MySqlFlywayTest.java` — MySQL + Flyway via Testcontainers.
- Test: `src/test/java/com/example/banking/infra/RedisTest.java` — Redis round-trip.
- Test: `src/test/java/com/example/banking/infra/KafkaTest.java` — Kafka publish/consume.
- Test: `src/test/java/com/example/banking/infra/AxonEventStoreTest.java` — Axon event store wired on MySQL.
- Test: `src/test/java/com/example/banking/architecture/HexagonalBoundaryTest.java` — ArchUnit rule.

---

### Task 1: Project scaffold — service boots with a health endpoint

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/example/banking/BankingApiApplication.java`
- Create: `src/main/resources/application.yml`
- Test: `src/test/java/com/example/banking/SmokeTest.java`

**Interfaces:**
- Produces: a runnable Spring Boot application `BankingApiApplication`; actuator health at `GET /actuator/health`.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/SmokeTest.java`
```java
package com.example.banking;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SmokeTest {

    @Autowired
    MockMvc mockMvc;

    @Test
    void healthEndpointIsUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}
```

- [ ] **Step 2: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.0</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>banking-api</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>banking-api</name>

    <properties>
        <java.version>26</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Create the application class**

`src/main/java/com/example/banking/BankingApiApplication.java`
```java
package com.example.banking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BankingApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(BankingApiApplication.class, args);
    }
}
```

- [ ] **Step 4: Create `application.yml`**

`src/main/resources/application.yml`
```yaml
spring:
  application:
    name: banking-api
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      show-details: always
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=SmokeTest > target/test-out.txt 2>&1; tail -n 40 target/test-out.txt`
Expected: `BUILD SUCCESS`, `SmokeTest.healthEndpointIsUp` passes. If the build fails on Java 26 toolchain resolution, this is the Global Constraint decision point — retry with `<java.version>25</java.version>` and record it.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/java/com/example/banking/BankingApiApplication.java src/main/resources/application.yml src/test/java/com/example/banking/SmokeTest.java
git commit -m "feat: scaffold Spring Boot 4.1 service with health endpoint"
```

---

### Task 2: MySQL + Flyway wired via Testcontainers

**Files:**
- Modify: `pom.xml` (add JPA, MySQL driver, Flyway, Testcontainers MySQL)
- Modify: `src/main/resources/application.yml` (datasource + flyway)
- Create: `src/main/resources/db/migration/V1__baseline.sql`
- Test: `src/test/java/com/example/banking/infra/MySqlFlywayTest.java`

**Interfaces:**
- Produces: a JPA `DataSource` on MySQL; Flyway migrations applied on startup.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/infra/MySqlFlywayTest.java`
```java
package com.example.banking.infra;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class MySqlFlywayTest {

    @Container
    @ServiceConnection
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:9.7");

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void flywayHistoryTableExists() {
        Integer migrations = jdbc.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history", Integer.class);
        assertThat(migrations).isGreaterThanOrEqualTo(1);
    }
}
```

- [ ] **Step 2: Add dependencies to `pom.xml`**

Add inside `<dependencies>`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-core</artifactId>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-mysql</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-testcontainers</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>mysql</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 3: Add datasource + Flyway config to `application.yml`**

Append under `spring:`
```yaml
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  flyway:
    enabled: true
    baseline-on-migrate: true
```
(No hard-coded datasource URL: local runs read it from `docker-compose.yml` env in Task 7; tests get it from `@ServiceConnection`.)

- [ ] **Step 4: Create the baseline migration**

`src/main/resources/db/migration/V1__baseline.sql`
```sql
-- Baseline migration. Projection tables are added by later milestones.
CREATE TABLE schema_marker (
    id TINYINT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_marker (id) VALUES (1);
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=MySqlFlywayTest > target/test-out.txt 2>&1; tail -n 40 target/test-out.txt`
Expected: PASS — a MySQL container starts, Flyway applies `V1`, `flyway_schema_history` has ≥1 row.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/resources/application.yml src/main/resources/db/migration/V1__baseline.sql src/test/java/com/example/banking/infra/MySqlFlywayTest.java
git commit -m "feat: wire MySQL datasource and Flyway with Testcontainers verification"
```

---

### Task 3: Redis connectivity

**Files:**
- Modify: `pom.xml` (Redis starter)
- Test: `src/test/java/com/example/banking/infra/RedisTest.java`

**Interfaces:**
- Produces: a `StringRedisTemplate` bean connected to Redis.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/infra/RedisTest.java`
```java
package com.example.banking.infra;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class RedisTest {

    @Container
    @ServiceConnection(name = "redis")
    static RedisContainer redis = new RedisContainer("redis:8.8");

    @Autowired
    StringRedisTemplate redisTemplate;

    @Test
    void redisRoundTrip() {
        redisTemplate.opsForValue().set("skeleton:key", "value");
        assertThat(redisTemplate.opsForValue().get("skeleton:key")).isEqualTo("value");
    }
}
```

- [ ] **Step 2: Add dependencies to `pom.xml`**

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
<dependency>
    <groupId>com.redis</groupId>
    <artifactId>testcontainers-redis</artifactId>
    <version>2.2.4</version>
    <scope>test</scope>
</dependency>
```
Note: `testcontainers-redis` is a community module; resolve its current version on Maven Central if `2.2.4` is stale. Alternatively use a raw `GenericContainer<>("redis:8.8").withExposedPorts(6379)` plus `@DynamicPropertySource` if a `@ServiceConnection`-compatible Redis module is unavailable.

- [ ] **Step 3: Run the test — verify it passes**

Run: `mvn -B test -Dtest=RedisTest > target/test-out.txt 2>&1; tail -n 40 target/test-out.txt`
Expected: PASS — Redis container starts, set/get round-trips.

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/test/java/com/example/banking/infra/RedisTest.java
git commit -m "feat: wire Redis with Testcontainers verification"
```

---

### Task 4: Kafka connectivity via spring-kafka

**Files:**
- Modify: `pom.xml` (`spring-kafka`, Testcontainers Kafka)
- Modify: `src/main/resources/application.yml` (kafka consumer group + offset)
- Test: `src/test/java/com/example/banking/infra/KafkaTest.java`

**Interfaces:**
- Produces: a `KafkaTemplate<String, String>` bean and a working `@KafkaListener` container factory.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/banking/infra/KafkaTest.java`
```java
package com.example.banking.infra;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class KafkaTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:latest"));

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    TestConsumer consumer;

    @Test
    void publishAndConsume() throws InterruptedException {
        kafkaTemplate.send("skeleton-topic", "hello");
        assertThat(consumer.latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(consumer.lastMessage).isEqualTo("hello");
    }

    @Component
    static class TestConsumer {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile String lastMessage;

        @KafkaListener(topics = "skeleton-topic", groupId = "skeleton-test")
        void receive(String message) {
            this.lastMessage = message;
            latch.countDown();
        }
    }
}
```

- [ ] **Step 2: Add dependencies to `pom.xml`**

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 3: Add Kafka consumer defaults to `application.yml`**

Append under `spring:`
```yaml
  kafka:
    consumer:
      auto-offset-reset: earliest
      group-id: banking-api
```

- [ ] **Step 4: Run the test — verify it passes**

Run: `mvn -B test -Dtest=KafkaTest > target/test-out.txt 2>&1; tail -n 40 target/test-out.txt`
Expected: PASS — Kafka container starts, message is produced and consumed within 10s.

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/main/resources/application.yml src/test/java/com/example/banking/infra/KafkaTest.java
git commit -m "feat: wire Kafka via spring-kafka with Testcontainers verification"
```

---

### Task 5: Axon Framework 5 event store on MySQL (the riskiest wiring)

> **SUPERSEDED (2026-07-24):** Axon was replaced by the DIY event-sourcing kernel before this task
> was ever built. Do not execute this task. See
> `docs/superpowers/specs/2026-07-24-diy-event-sourcing-design.md` and
> `docs/superpowers/plans/2026-07-24-diy-event-sourcing-kernel.md`.

**Files:**
- Modify: `pom.xml` (Axon 5 starter, Axon Server excluded/disabled)
- Modify: `src/main/resources/application.yml` (`axon.axonserver.enabled=false`)
- Test: `src/test/java/com/example/banking/infra/AxonEventStoreTest.java`

**Interfaces:**
- Produces: an Axon `EventStore` / `EventStorageEngine` bean backed by MySQL (embedded store, not Axon Server).

- [ ] **Step 1: Resolve the current Axon 5 coordinates (spike — do this first)**

Axon 5 is preview-stage; do NOT trust a version string from this plan. Determine the current coordinates:

Run: `curl -s "https://search.maven.org/solrsearch/select?q=g:org.axonframework+AND+a:axon-spring-boot-starter&core=gav&rows=20&wt=json" > target/axon-versions.json; cat target/axon-versions.json`

Record the highest `5.x` version returned. If the 5.x line has moved to a different groupId (e.g. an `org.axoniq*` group), use that instead. Note the chosen coordinate in the commit message.

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/infra/AxonEventStoreTest.java`
```java
package com.example.banking.infra;

import org.axonframework.eventsourcing.eventstore.EventStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class AxonEventStoreTest {

    @Container
    @ServiceConnection
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:9.7");

    @Autowired(required = false)
    EventStore eventStore;

    @Test
    void embeddedEventStoreIsWiredOnMysql() {
        assertThat(eventStore)
                .as("Axon EventStore bean should be present and not the Axon Server variant")
                .isNotNull();
        assertThat(eventStore.getClass().getName())
                .doesNotContain("axonserver");
    }
}
```
Note: `EventStore` is a long-standing Axon interface; if the Axon 5 API has renamed it, resolve the correct type from the coordinates found in Step 1 and adjust the import + assertion. The intent — *an embedded MySQL-backed store is wired, not Axon Server* — does not change.

- [ ] **Step 3: Add the Axon 5 starter to `pom.xml`**

Use the coordinate resolved in Step 1 (shown here with a placeholder version to be replaced):
```xml
<dependency>
    <groupId>org.axonframework</groupId>
    <artifactId>axon-spring-boot-starter</artifactId>
    <version>REPLACE_WITH_RESOLVED_5x_VERSION</version>
    <exclusions>
        <exclusion>
            <groupId>org.axonframework</groupId>
            <artifactId>axon-server-connector</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```
JPA is already on the classpath (Task 2), so Axon auto-configures a JPA event storage engine on MySQL.

- [ ] **Step 4: Disable Axon Server in `application.yml`**

Append at top level:
```yaml
axon:
  axonserver:
    enabled: false
```

- [ ] **Step 5: Run the test — verify it passes**

Run: `mvn -B test -Dtest=AxonEventStoreTest > target/test-out.txt 2>&1; tail -n 60 target/test-out.txt`
Expected: PASS — context loads with an embedded JPA event store; Axon event tables (e.g. `domain_event_entry`) are created against MySQL. If autoconfiguration still references an Axon Server bean despite the exclusion, the `axon.axonserver.enabled=false` property is the documented fix; confirm both the exclusion and the property are present.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/resources/application.yml src/test/java/com/example/banking/infra/AxonEventStoreTest.java
git commit -m "feat: wire Axon 5 embedded event store on MySQL (version <resolved>)"
```

---

### Task 6: ArchUnit hexagonal boundary rule

**Files:**
- Modify: `pom.xml` (ArchUnit)
- Create: `src/main/java/com/example/banking/domain/package-info.java`
- Create: `src/main/java/com/example/banking/domain/Marker.java`
- Create: `src/main/java/com/example/banking/adapter/in/web/HealthNote.java`
- Test: `src/test/java/com/example/banking/architecture/HexagonalBoundaryTest.java`

**Interfaces:**
- Produces: an enforced rule that `..domain..` must not depend on `..adapter..`.

- [ ] **Step 1: Create the two marker classes so the rule has real classes to evaluate**

`src/main/java/com/example/banking/domain/package-info.java`
```java
package com.example.banking.domain;
```

`src/main/java/com/example/banking/domain/Marker.java`
```java
package com.example.banking.domain;

/** Placeholder domain type so the architecture rule evaluates a non-empty domain package.
 *  Replaced by real aggregates in milestone 1. */
public final class Marker {
    private Marker() {
    }
}
```

`src/main/java/com/example/banking/adapter/in/web/HealthNote.java`
```java
package com.example.banking.adapter.in.web;

/** Placeholder adapter type. Replaced by real controllers in milestone 1. */
public final class HealthNote {
    private HealthNote() {
    }
}
```

- [ ] **Step 2: Write the failing test**

`src/test/java/com/example/banking/architecture/HexagonalBoundaryTest.java`
```java
package com.example.banking.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class HexagonalBoundaryTest {

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.example.banking");

    @Test
    void domainMustNotDependOnAdapters() {
        noClasses()
                .that().resideInAPackage("..domain..")
                .should().dependOnClassesThat().resideInAPackage("..adapter..")
                .check(classes);
    }
}
```

- [ ] **Step 3: Add ArchUnit to `pom.xml`**

```xml
<dependency>
    <groupId>com.tngtech.archunit</groupId>
    <artifactId>archunit-junit5</artifactId>
    <version>1.4.0</version>
    <scope>test</scope>
</dependency>
```
Resolve the current `archunit-junit5` version on Maven Central if `1.4.0` is stale.

- [ ] **Step 4: Run the test — verify it passes**

Run: `mvn -B test -Dtest=HexagonalBoundaryTest > target/test-out.txt 2>&1; tail -n 40 target/test-out.txt`
Expected: PASS — the rule evaluates the real `domain` package and finds no forbidden dependency.

- [ ] **Step 5: Prove the rule can fail (temporary check — do not commit this edit)**

Temporarily add `import com.example.banking.adapter.in.web.HealthNote;` and a field `HealthNote note;` to `Marker.java`, re-run Step 4, and confirm the test now FAILS with an architecture violation. Then revert the edit and re-run to confirm PASS again. This proves the guard is live, not vacuous.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/java/com/example/banking/domain/package-info.java src/main/java/com/example/banking/domain/Marker.java src/main/java/com/example/banking/adapter/in/web/HealthNote.java src/test/java/com/example/banking/architecture/HexagonalBoundaryTest.java
git commit -m "feat: enforce hexagonal domain-to-adapter boundary with ArchUnit"
```

---

### Task 7: Local run — docker-compose, README, full green build

**Files:**
- Create: `docker-compose.yml`
- Create: `README.md`
- Modify: `src/main/resources/application.yml` (local connection defaults via env)

**Interfaces:**
- Produces: `docker compose up` brings up MySQL + Redis + Kafka; `mvn spring-boot:run` runs the app against them; `mvn verify` runs all skeleton tests green.

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  mysql:
    image: mysql:9.7
    environment:
      MYSQL_DATABASE: banking
      MYSQL_ROOT_PASSWORD: root
    ports:
      - "3306:3306"
  redis:
    image: redis:8.8
    ports:
      - "6379:6379"
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
```

- [ ] **Step 2: Add local connection defaults to `application.yml`**

Append under `spring:` (tests override these via Testcontainers; these are for local `spring-boot:run`):
```yaml
  datasource:
    url: jdbc:mysql://${DB_HOST:localhost}:3306/banking
    username: root
    password: root
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: 6379
  kafka:
    bootstrap-servers: ${KAFKA_HOST:localhost}:9092
```

- [ ] **Step 3: Create `README.md`**

```markdown
# banking-api

Core banking API — see `ARCHITECTURE.md` and `docs/superpowers/specs/` for design.

## Run locally
    docker compose up -d
    mvn spring-boot:run
Health: http://localhost:8080/actuator/health

## Test
    mvn verify
Integration tests use Testcontainers, so a running Docker/Podman daemon is required.
```

- [ ] **Step 4: Run the full build — verify all skeleton tests pass**

Run: `mvn -B verify > target/verify-out.txt 2>&1; tail -n 60 target/verify-out.txt`
Expected: `BUILD SUCCESS` with `SmokeTest`, `MySqlFlywayTest`, `RedisTest`, `KafkaTest`, `AxonEventStoreTest`, and `HexagonalBoundaryTest` all passing.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml README.md src/main/resources/application.yml
git commit -m "chore: add docker-compose, README, and local run configuration"
```

---

## Definition of done (Milestone 0)

- `mvn verify` is green on Java 26 (or Java 25 with the fallback recorded).
- MySQL, Redis, and Kafka each proven to connect via a Testcontainers test.
- Axon 5 embedded event store confirmed wired on MySQL, Axon Server disabled, with the resolved 5.x coordinate recorded.
- ArchUnit domain→adapter rule proven to both pass and fail.
- App runs locally against `docker-compose`.
- Any bleeding-edge deviation (Java version fallback, Axon coordinate/groupId, community module versions) is written into the commit history so milestone 1 planning starts from proven facts.

## Self-review notes

- **Spec coverage:** This plan intentionally covers only the NFR/infra foundation (hexagonal boundary, Axon event store, Kafka bus, MySQL/Redis). All functional requirements are deferred to milestones 1–7, listed in the roadmap. No functional requirement is silently dropped.
- **Placeholders:** The single deliberate placeholder is the Axon version string, which is a *resolve-at-build-time* instruction (Task 5, Step 1), not an unspecified requirement — Axon 5 coordinates cannot be honestly hard-coded from a preview state.
- **Type consistency:** Package names (`com.example.banking.domain`, `..adapter.in.web`), the `EventStore` type, and bean names are used consistently across tasks.
