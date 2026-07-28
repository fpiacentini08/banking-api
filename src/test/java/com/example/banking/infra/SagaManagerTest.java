package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.saga.JooqSagaStore;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.command.CommandHandler;
import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.saga.DeadlineRequest;
import com.example.banking.eventsourcing.saga.DeadlineScheduler;
import com.example.banking.eventsourcing.common.DomainError;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.saga.SagaManager;
import com.example.banking.eventsourcing.saga.SagaStore;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.support.SequentialIds;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.jooq.DSLContext;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import io.vavr.control.Either;

import static com.example.banking.eventsourcing.support.TransferLikeSaga.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(ContainersConfig.class)
class SagaManagerTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;

    SagaStore sagaStore;
    SagaManager<TransferLikeSaga.State> manager;
    List<Object> dispatched;
    List<String> scheduled;
    List<String> cancelled;
    EventSerializer serializer;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM saga_instance");
        jdbc.update("DELETE FROM saga_association");
        ObjectMapper mapper = new ObjectMapper();
        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("Requested", 1, Requested.class);
        registry.register("Debited", 1, Debited.class);
        registry.register("Credited", 1, Credited.class);
        serializer = new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), Clock.systemUTC(),
                new SequentialIds("saga-manager-event"));
        dispatched = new CopyOnWriteArrayList<>();
        scheduled = new CopyOnWriteArrayList<>();
        cancelled = new CopyOnWriteArrayList<>();
        CommandBus recordingBus = new CommandBus() {
            @Override public <C> void register(Class<C> type, Function<C, String> idOf, CommandHandler<C> handler) {}
            @Override public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
                dispatched.add(command);
                return CompletableFuture.completedFuture(Either.right(new CommittedEvents("x", 0, List.of())));
            }
        };
        DeadlineScheduler recordingDeadlines = new DeadlineScheduler() {
            @Override public void schedule(String sagaType, String sagaId, DeadlineRequest request) {
                scheduled.add(request.deadlineId());
            }
            @Override public void cancel(String deadlineId) { cancelled.add(deadlineId); }
        };
        sagaStore = new JooqSagaStore(dsl);
        manager = new SagaManager<>(new TransferLikeSaga(), sagaStore, new JacksonPayloadCodec(mapper),
                TransferLikeSaga.State.class, serializer, recordingBus, recordingDeadlines,
                new SequentialIds("saga-manager-saga"));
    }

    private StoredEvent stored(Object event, long position) {
        SerializedEvent serialized = serializer.serialize(event, Map.of());
        return new StoredEvent(position, "Transfer", "t-agg", position - 1, serialized);
    }

    @Test
    void startsPersistsCorrelatesAndFinishesASaga() {
        manager.handle(stored(new Requested("t-1"), 1));

        assertThat(dispatched).containsExactly(new DebitCmd("t-1"));
        assertThat(scheduled).containsExactly("timeout-t-1");
        assertThat(sagaStore.findByAssociation("TransferLike", "t-1")).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isFalse());

        manager.handle(stored(new Debited("t-1"), 2));
        manager.handle(stored(new Credited("t-1"), 3));

        assertThat(dispatched).containsExactly(new DebitCmd("t-1"), new CreditCmd("t-1"));
        assertThat(cancelled).containsExactly("timeout-t-1");
        assertThat(sagaStore.findByAssociation("TransferLike", "t-1")).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isTrue());
    }

    @Test
    void terminalSagaIgnoresFurtherEvents() {
        manager.handle(stored(new Requested("t-2"), 1));
        manager.handle(stored(new Debited("t-2"), 2));
        manager.handle(stored(new Credited("t-2"), 3));
        int commandCount = dispatched.size();

        manager.handle(stored(new Debited("t-2"), 4));

        assertThat(dispatched).hasSize(commandCount);
    }

    @Test
    void nonStartingEventWithoutExistingSagaIsIgnored() {
        manager.handle(stored(new Debited("t-3"), 1));

        assertThat(dispatched).isEmpty();
        assertThat(sagaStore.findByAssociation("TransferLike", "t-3")).isEmpty();
    }
}
