package com.example.banking.architecture;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The kernel replaces Axon; unlike Axon annotations, it grants the domain no framework
 * touchpoint. These rules keep both the kernel and the domain free of infrastructure.
 * Enforced by the DIY ArchCheck scanner (JDK ClassFile API) — no ArchUnit.
 */
class EventSourcingKernelRulesTest {

    static ArchCheck check;

    @BeforeAll
    static void scanCompiledClasses() {
        check = ArchCheck.scan(Path.of("target", "classes"));
    }

    @Test
    void scannerSeesKnownDependencies() {
        // guard against a silently empty or broken scan: the JDBC adapter must reference Spring
        assertThat(check.referenceExists("com.example.banking.adapter.out.eventstore", "org.springframework"))
                .as("ArchCheck should see JooqEventStore -> Spring; empty scan means the checker is broken")
                .isTrue();
    }

    @Test
    void kernelDependsOnlyOnItselfVavrAndJava() {
        List<ArchCheck.Violation> violations = check.violations(
                "com.example.banking.eventsourcing.",
                List.of("com.example.banking.eventsourcing.", "io.vavr.", "java."));

        assertThat(violations).isEmpty();
    }

    @Test
    void domainDependsOnlyOnItselfKernelVavrAndJava() {
        List<ArchCheck.Violation> violations = check.violations(
                "com.example.banking.domain.",
                List.of("com.example.banking.domain.", "com.example.banking.eventsourcing.",
                        "io.vavr.", "java."));

        assertThat(violations).isEmpty();
    }

    @Test
    void jooqStaysOutOfDomainApplicationAndWeb() {
        // Database access is jOOQ-only, confined to adapter.out.* Repository classes. The domain,
        // application, and web layers depend on those Repositories and never touch org.jooq / SQL.
        assertThat(check.referenceExists("com.example.banking.domain.", "org.jooq"))
                .as("domain must not reference org.jooq")
                .isFalse();
        assertThat(check.referenceExists("com.example.banking.application.", "org.jooq"))
                .as("application must not reference org.jooq")
                .isFalse();
        assertThat(check.referenceExists("com.example.banking.adapter.in.web.", "org.jooq"))
                .as("adapter.in.web must not reference org.jooq")
                .isFalse();
    }
}
