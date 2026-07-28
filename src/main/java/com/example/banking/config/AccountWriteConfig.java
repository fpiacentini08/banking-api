package com.example.banking.config;

import com.example.banking.adapter.out.eventstore.config.EventSourcingProperties;
import com.example.banking.adapter.out.projection.AccountBalanceProjection;
import com.example.banking.adapter.out.projection.AccountBalanceRepository;
import com.example.banking.domain.account.Account;
import com.example.banking.domain.account.AccountBehaviour;
import com.example.banking.domain.account.AccountCommand;
import com.example.banking.domain.account.AccountEvent;
import com.example.banking.eventsourcing.aggregate.EventSourcingRepository;
import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.common.PayloadCodec;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.processor.TokenStore;
import com.example.banking.eventsourcing.processor.TrackingProcessor;
import com.example.banking.eventsourcing.snapshot.SnapshotStore;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the Account write path onto the event-sourcing kernel: event-type registration, the Account
 * aggregate repository, the OpenAccount command handler, and the auto-started account_balance processor.
 */
@Configuration(proxyBeanMethods = false)
public class AccountWriteConfig {

    static final String ACCOUNT_BALANCE_PROCESSOR = "account-balance-projection";

    @Bean
    EventSourcingRepository<Account, AccountCommand, AccountEvent> accountRepository(
            EventStore eventStore, SnapshotStore snapshotStore, EventSerializer eventSerializer,
            PayloadCodec codec, EventSourcingProperties properties) {
        return new EventSourcingRepository<>(new AccountBehaviour(), eventStore, snapshotStore,
                eventSerializer, codec, Account.class, 1,
                properties.snapshotThreshold(), properties.maxCommandAttempts());
    }

    @Bean
    AccountWriteModelRegistrar accountWriteModelRegistrar(
            EventTypeRegistry registry, CommandBus commandBus,
            EventSourcingRepository<Account, AccountCommand, AccountEvent> accountRepository) {
        return new AccountWriteModelRegistrar(registry, commandBus, accountRepository);
    }

    @Bean
    AccountBalanceRepository accountBalanceRepository(DSLContext dsl) {
        return new AccountBalanceRepository(dsl);
    }

    @Bean
    TrackingProcessor accountBalanceProcessor(
            EventStore eventStore, TokenStore tokenStore, TransactionalRunner tx,
            EventSerializer eventSerializer, AccountBalanceRepository accountBalanceRepository,
            EventSourcingProperties properties) {
        AccountBalanceProjection projection =
                new AccountBalanceProjection(accountBalanceRepository, eventSerializer);
        return new TrackingProcessor(ACCOUNT_BALANCE_PROCESSOR, eventStore, tokenStore, tx, projection,
                properties.batchSize(), properties.pollInterval());
    }

    @Bean
    TrackingProcessorLifecycle accountBalanceLifecycle(TrackingProcessor accountBalanceProcessor) {
        return new TrackingProcessorLifecycle(accountBalanceProcessor);
    }
}
