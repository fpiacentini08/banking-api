package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.saga.DeadlinePoller;
import com.example.banking.adapter.out.eventstore.saga.DeadlineRepository;
import com.example.banking.adapter.out.eventstore.serialization.JacksonEventSerializer;
import com.example.banking.adapter.out.eventstore.serialization.JacksonPayloadCodec;
import com.example.banking.adapter.out.eventstore.saga.JooqDeadlineScheduler;
import com.example.banking.adapter.out.eventstore.saga.JooqSagaStore;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;
import com.example.banking.adapter.out.eventstore.serialization.UpcasterChain;
import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.command.CommandHandler;
import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.saga.DeadlineRequest;
import com.example.banking.eventsourcing.common.DomainError;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.common.PayloadCodec;
import com.example.banking.eventsourcing.saga.SagaInstance;
import com.example.banking.eventsourcing.saga.SagaManager;
import com.example.banking.eventsourcing.support.SequentialIds;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.jooq.DSLContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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
class DeadlineTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;

    Clock clock = Clock.fixed(Instant.parse("2026-07-24T10:00:00Z"), ZoneOffset.UTC);
    PayloadCodec codec;
    EventTypeRegistry registry;
    JooqDeadlineScheduler scheduler;
    DeadlinePoller poller;
    JooqSagaStore sagaStore;
    List<Object> dispatched;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM deadline");
        jdbc.update("DELETE FROM saga_instance");
        jdbc.update("DELETE FROM saga_association");
        ObjectMapper mapper = new ObjectMapper();
        codec = new JacksonPayloadCodec(mapper);
        registry = new EventTypeRegistry();
        registry.register("TimedOut", 1, TimedOut.class);
        scheduler = new JooqDeadlineScheduler(dsl, codec, registry, clock);
        sagaStore = new JooqSagaStore(dsl);
        dispatched = new CopyOnWriteArrayList<>();
        CommandBus recordingBus = new CommandBus() {
            @Override public <C> void register(Class<C> type, Function<C, String> idOf, CommandHandler<C> handler) {}
            @Override public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
                dispatched.add(command);
                return CompletableFuture.completedFuture(Either.right(new CommittedEvents("x", 0, List.of())));
            }
        };
        SagaManager<TransferLikeSaga.State> manager = new SagaManager<>(new TransferLikeSaga(),
                sagaStore, codec, TransferLikeSaga.State.class,
                new JacksonEventSerializer(mapper, registry, new UpcasterChain(List.of()), clock,
                        new SequentialIds("deadline-event")),
                recordingBus, scheduler, new SequentialIds("deadline-saga"));
        poller = new DeadlinePoller(new DeadlineRepository(dsl),
                new SpringTransactionalRunner(new TransactionTemplate(txManager)),
                codec, registry, Map.of("TransferLike", manager), clock, Duration.ofMillis(50));
    }

    private String activeSaga(String txId) {
        String sagaId = java.util.UUID.randomUUID().toString();
        sagaStore.insert(new SagaInstance(sagaId, "TransferLike",
                codec.encode(new State(txId, Phase.CREDITING)), false), txId);
        return sagaId;
    }

    @Test
    void dueDeadlineIsDeliveredToTheSagaAndDeleted() {
        String sagaId = activeSaga("t-1");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-1", Duration.ZERO, new TimedOut("t-1")));

        int delivered = poller.pollOnce();

        assertThat(delivered).isEqualTo(1);
        assertThat(dispatched).containsExactly(new RefundCmd("t-1"));
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deadline", Long.class)).isZero();
        assertThat(sagaStore.findById(sagaId)).hasValueSatisfying(saga ->
                assertThat(saga.terminal()).isTrue());
    }

    @Test
    void futureDeadlineIsNotDelivered() {
        String sagaId = activeSaga("t-2");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-2", Duration.ofMinutes(5), new TimedOut("t-2")));

        assertThat(poller.pollOnce()).isZero();
        assertThat(dispatched).isEmpty();
    }

    @Test
    void cancelledDeadlineIsNeverDelivered() {
        String sagaId = activeSaga("t-3");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-3", Duration.ZERO, new TimedOut("t-3")));
        scheduler.cancel("timeout-t-3");

        assertThat(poller.pollOnce()).isZero();
        assertThat(dispatched).isEmpty();
    }

    @Test
    void deadlineForTerminalSagaIsDeletedWithoutEffect() {
        String sagaId = java.util.UUID.randomUUID().toString();
        sagaStore.insert(new SagaInstance(sagaId, "TransferLike",
                codec.encode(new State("t-4", Phase.DONE)), true), "t-4");
        scheduler.schedule("TransferLike", sagaId,
                new DeadlineRequest("timeout-t-4", Duration.ZERO, new TimedOut("t-4")));

        poller.pollOnce();

        assertThat(dispatched).isEmpty();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deadline", Long.class)).isZero();
    }
}
