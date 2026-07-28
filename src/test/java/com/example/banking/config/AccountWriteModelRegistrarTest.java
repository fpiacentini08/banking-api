package com.example.banking.config;

import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.domain.account.Account;
import com.example.banking.domain.account.AccountAlreadyOpened;
import com.example.banking.domain.account.AccountBehaviour;
import com.example.banking.domain.account.AccountCommand;
import com.example.banking.domain.account.AccountEvent;
import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.AccountOpened;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.domain.user.UserId;
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

class AccountWriteModelRegistrarTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final EventTypeRegistry registry = new EventTypeRegistry();
    private final StripedCommandBus commandBus = new StripedCommandBus(4);
    private final InMemoryEventStore eventStore = new InMemoryEventStore();

    private final EventSourcingRepository<Account, AccountCommand, AccountEvent> repository =
            new EventSourcingRepository<>(new AccountBehaviour(), eventStore, new InMemorySnapshotStore(),
                    new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()),
                            Clock.systemUTC(), new SequentialIds("account-registrar-event")),
                    new JacksonPayloadCodec(mapper), Account.class, 1, 100, 3);

    private final AccountWriteModelRegistrar registrar =
            new AccountWriteModelRegistrar(registry, commandBus, repository);

    @AfterEach
    void tearDown() {
        commandBus.close();
    }

    @Test
    void registersAccountOpenedEventType() {
        registrar.afterPropertiesSet();

        assertThat(registry.byClass(AccountOpened.class))
                .isEqualTo(new EventTypeRegistry.EventType("AccountOpened", 1, AccountOpened.class));
    }

    @Test
    void registersOpenAccountHandlerRoutedByAccountId() throws Exception {
        registrar.afterPropertiesSet();
        AccountId accountId = new AccountId("account-registrar-1");
        UserId ownerId = new UserId("account-registrar-owner-1");

        Either<DomainError, CommittedEvents> result =
                commandBus.dispatch(new OpenAccount(accountId, ownerId)).get();

        assertThat(result.get()).isEqualTo(new CommittedEvents(accountId.value(), 0,
                List.of(new AccountOpened(accountId, ownerId))));
        assertThat(eventStore.readStream(accountId.value(), -1)).hasSize(1);
    }

    @Test
    void registeredHandlerRehydratesAndRejectsAReopening() throws Exception {
        registrar.afterPropertiesSet();
        AccountId accountId = new AccountId("account-registrar-2");
        OpenAccount command = new OpenAccount(accountId, new UserId("account-registrar-owner-2"));
        commandBus.dispatch(command).get();

        Either<DomainError, CommittedEvents> result = commandBus.dispatch(command).get();

        assertThat(result.getLeft()).isEqualTo(new AccountAlreadyOpened(accountId));
        assertThat(eventStore.readStream(accountId.value(), -1)).hasSize(1);
    }

    @Test
    void nothingIsRegisteredBeforeAfterPropertiesSet() {
        assertThatIllegalArgumentException().isThrownBy(() -> registry.byClass(AccountOpened.class));
    }
}
