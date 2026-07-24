package com.example.banking.eventsourcing;

import io.vavr.control.Either;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class StripedCommandBusTest {

    record TestCommand(String aggregateId, int seq) {}
    record OtherCommand(String aggregateId) {}

    private final StripedCommandBus bus = new StripedCommandBus(4);

    @AfterEach
    void tearDown() {
        bus.close();
    }

    @Test
    void sameAggregateCommandsExecuteInDispatchOrder() throws Exception {
        List<Integer> observed = new CopyOnWriteArrayList<>();
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> {
            observed.add(command.seq());
            return Either.right(new CommittedEvents(command.aggregateId(), command.seq(), List.of()));
        });

        List<CompletableFuture<Either<DomainError, CommittedEvents>>> futures =
                java.util.stream.IntStream.range(0, 100)
                        .mapToObj(i -> bus.dispatch(new TestCommand("same-aggregate", i)))
                        .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();

        assertThat(observed).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, 100).boxed().toList());
    }

    @Test
    void leftOutcomePassesThrough() throws Exception {
        DomainError error = new DomainError() {
            @Override public String code() { return "test.error"; }
            @Override public String message() { return "boom"; }
        };
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> Either.left(error));

        Either<DomainError, CommittedEvents> outcome = bus.dispatch(new TestCommand("a", 0)).get();

        assertThat(outcome.getLeft().code()).isEqualTo("test.error");
    }

    @Test
    void handlerExceptionFailsTheFuture() {
        bus.register(TestCommand.class, TestCommand::aggregateId, command -> {
            throw new ConcurrencyConflict("a", 0);
        });

        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> bus.dispatch(new TestCommand("a", 0)).get())
                .withCauseInstanceOf(ConcurrencyConflict.class);
    }

    @Test
    void unregisteredCommandFailsTheFuture() {
        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> bus.dispatch(new OtherCommand("a")).get())
                .withCauseInstanceOf(IllegalArgumentException.class);
    }
}
