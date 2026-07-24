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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
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
