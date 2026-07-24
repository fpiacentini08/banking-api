package com.example.banking.infra;

import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.saga.DeadlineScheduler;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.saga.SagaStore;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import com.example.banking.eventsourcing.processor.TokenStore;
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
