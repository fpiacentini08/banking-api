package com.example.banking.config;

import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.User;
import com.example.banking.domain.user.UserBehaviour;
import com.example.banking.domain.user.UserCommand;
import com.example.banking.domain.user.UserEvent;
import com.example.banking.domain.user.UserAlreadyRegistered;
import com.example.banking.domain.user.UserId;
import com.example.banking.domain.user.UserRegistered;
import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.aggregate.EventSourcingRepository;
import com.example.banking.eventsourcing.command.StripedCommandBus;
import com.example.banking.eventsourcing.common.DomainError;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.support.InMemoryEventStore;
import com.example.banking.eventsourcing.support.InMemorySnapshotStore;
import com.example.banking.eventsourcing.support.SequentialIds;
import io.vavr.control.Either;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class UserWriteModelRegistrarTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventTypeRegistry registry = new EventTypeRegistry();
    private final StripedCommandBus commandBus = new StripedCommandBus(4);
    private final InMemoryEventStore eventStore = new InMemoryEventStore();

    private final EventSourcingRepository<User, UserCommand, UserEvent> repository =
            new EventSourcingRepository<>(new UserBehaviour(), eventStore, new InMemorySnapshotStore(),
                    new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()),
                            Clock.systemUTC(), new SequentialIds("user-registrar-event")),
                    new JacksonPayloadCodec(mapper), User.class, 1, 100, 3);

    private final UserWriteModelRegistrar registrar =
            new UserWriteModelRegistrar(registry, commandBus, repository);

    @AfterEach
    void tearDown() {
        commandBus.close();
    }

    @Test
    void registersUserRegisteredEventType() {
        registrar.afterPropertiesSet();

        assertThat(registry.byClass(UserRegistered.class))
                .isEqualTo(new EventTypeRegistry.EventType("UserRegistered", 1, UserRegistered.class));
    }

    @Test
    void registersRegisterUserHandlerRoutedByUserId() throws Exception {
        registrar.afterPropertiesSet();
        UserId userId = new UserId("user-registrar-1");

        Either<DomainError, CommittedEvents> result = commandBus
                .dispatch(new RegisterUser(userId, "Ada Lovelace", "ada@example.com")).get();

        assertThat(result.get()).isEqualTo(new CommittedEvents(userId.value(), 0,
                List.of(new UserRegistered(userId, "Ada Lovelace", "ada@example.com"))));
        assertThat(eventStore.readStream(userId.value(), -1)).hasSize(1);
    }

    @Test
    void registeredHandlerRehydratesAndRejectsAReRegistration() throws Exception {
        registrar.afterPropertiesSet();
        UserId userId = new UserId("user-registrar-2");
        RegisterUser command = new RegisterUser(userId, "Ada Lovelace", "ada@example.com");
        commandBus.dispatch(command).get();

        Either<DomainError, CommittedEvents> result = commandBus.dispatch(command).get();

        assertThat(result.getLeft()).isEqualTo(new UserAlreadyRegistered(userId));
        assertThat(eventStore.readStream(userId.value(), -1)).hasSize(1);
    }

    @Test
    void nothingIsRegisteredBeforeAfterPropertiesSet() {
        assertThatIllegalArgumentException().isThrownBy(() -> registry.byClass(UserRegistered.class));
    }
}
