package com.example.banking.config;

import com.example.banking.adapter.out.eventstore.config.EventSourcingProperties;
import com.example.banking.adapter.out.projection.UsersProjection;
import com.example.banking.adapter.out.projection.UsersRepository;
import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.User;
import com.example.banking.domain.user.UserBehaviour;
import com.example.banking.domain.user.UserCommand;
import com.example.banking.domain.user.UserEvent;
import com.example.banking.domain.user.UserRegistered;
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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the User write path onto the event-sourcing kernel: event-type registration, the User
 * aggregate repository, the RegisterUser command handler, and the auto-started users read-model
 * processor.
 */
@Configuration(proxyBeanMethods = false)
public class UserWriteConfig {

    static final String USERS_PROCESSOR = "users-projection";

    @Bean
    EventSourcingRepository<User, UserCommand, UserEvent> userRepository(
            EventStore eventStore, SnapshotStore snapshotStore, EventSerializer eventSerializer,
            PayloadCodec codec, EventSourcingProperties properties) {
        return new EventSourcingRepository<>(new UserBehaviour(), eventStore, snapshotStore,
                eventSerializer, codec, User.class, 1,
                properties.snapshotThreshold(), properties.maxCommandAttempts());
    }

    @Bean
    InitializingBean registerUserEventTypes(EventTypeRegistry registry) {
        return () -> registry.register("UserRegistered", 1, UserRegistered.class);
    }

    @Bean
    InitializingBean registerUserCommandHandler(
            CommandBus commandBus, EventSourcingRepository<User, UserCommand, UserEvent> userRepository) {
        return () -> commandBus.register(RegisterUser.class, command -> command.userId().value(),
                command -> userRepository.execute(command.userId().value(), command));
    }

    @Bean
    UsersRepository usersRepository(DSLContext dsl) {
        return new UsersRepository(dsl);
    }

    @Bean
    TrackingProcessor usersProjectionProcessor(
            EventStore eventStore, TokenStore tokenStore, TransactionalRunner tx,
            EventSerializer eventSerializer, UsersRepository usersRepository, EventSourcingProperties properties) {
        UsersProjection projection = new UsersProjection(usersRepository, eventSerializer);
        return new TrackingProcessor(USERS_PROCESSOR, eventStore, tokenStore, tx, projection,
                properties.batchSize(), properties.pollInterval());
    }

    @Bean
    TrackingProcessorLifecycle usersProjectionLifecycle(TrackingProcessor usersProjectionProcessor) {
        return new TrackingProcessorLifecycle(usersProjectionProcessor);
    }
}
