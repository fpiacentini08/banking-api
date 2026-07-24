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
