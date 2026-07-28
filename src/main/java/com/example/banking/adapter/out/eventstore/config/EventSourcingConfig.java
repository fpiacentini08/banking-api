package com.example.banking.adapter.out.eventstore.config;

import com.example.banking.adapter.out.id.UuidEventIdGenerator;
import com.example.banking.adapter.out.id.UuidSagaIdGenerator;
import com.example.banking.adapter.out.eventstore.saga.JooqDeadlineScheduler;
import com.example.banking.adapter.out.eventstore.saga.JooqSagaStore;
import com.example.banking.adapter.out.eventstore.processor.JooqTokenStore;
import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.adapter.out.eventstore.snapshot.JooqSnapshotStore;
import com.example.banking.adapter.out.eventstore.store.JooqEventStore;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;

import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.saga.DeadlineScheduler;
import com.example.banking.eventsourcing.saga.SagaIdGenerator;
import com.example.banking.eventsourcing.event.EventIdGenerator;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.common.PayloadCodec;
import com.example.banking.eventsourcing.saga.SagaStore;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import com.example.banking.eventsourcing.command.StripedCommandBus;
import com.example.banking.eventsourcing.processor.TokenStore;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.jooq.DSLContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

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
    EventIdGenerator eventIdGenerator() {
        return new UuidEventIdGenerator();
    }

    @Bean
    SagaIdGenerator sagaIdGenerator() {
        return new UuidSagaIdGenerator();
    }

    @Bean
    EventSerializer eventSerializer(ObjectMapper mapper, EventTypeRegistry registry,
                                    UpcasterChain upcasters, Clock kernelClock,
                                    EventIdGenerator eventIdGenerator) {
        return new JacksonEventSerializer(mapper, registry, upcasters, kernelClock, eventIdGenerator);
    }

    @Bean
    TransactionalRunner transactionalRunner(PlatformTransactionManager transactionManager) {
        return new SpringTransactionalRunner(new TransactionTemplate(transactionManager));
    }

    @Bean
    EventStore eventStore(DSLContext dsl, TransactionalRunner tx) {
        return new JooqEventStore(dsl, tx);
    }

    @Bean
    SnapshotStore snapshotStore(DSLContext dsl) {
        return new JooqSnapshotStore(dsl);
    }

    @Bean
    TokenStore tokenStore(DSLContext dsl) {
        return new JooqTokenStore(dsl);
    }

    @Bean
    SagaStore sagaStore(DSLContext dsl) {
        return new JooqSagaStore(dsl);
    }

    @Bean(destroyMethod = "close")
    CommandBus commandBus(EventSourcingProperties properties) {
        return new StripedCommandBus(properties.commandStripes());
    }

    @Bean
    DeadlineScheduler deadlineScheduler(DSLContext dsl, PayloadCodec codec,
                                        EventTypeRegistry registry, Clock kernelClock) {
        return new JooqDeadlineScheduler(dsl, codec, registry, kernelClock);
    }
}
