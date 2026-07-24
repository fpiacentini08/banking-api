package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.AggregateBehaviour;
import com.example.banking.eventsourcing.DomainError;
import io.vavr.control.Either;

import java.util.List;

/** Toy aggregate used by kernel tests only: a counter that must never go below zero. */
public final class CounterBehaviour
        implements AggregateBehaviour<CounterBehaviour.Counter, CounterBehaviour.CounterCommand, CounterBehaviour.CounterEvent> {

    public sealed interface CounterCommand permits Increment, Decrement {
        String counterId();
    }
    public record Increment(String counterId, int by) implements CounterCommand {}
    public record Decrement(String counterId, int by) implements CounterCommand {}

    public sealed interface CounterEvent permits Incremented, Decremented {}
    public record Incremented(int by) implements CounterEvent {}
    public record Decremented(int by) implements CounterEvent {}

    public record Counter(int value) {}

    public record NegativeCounter(int value, int attempted) implements DomainError {
        @Override public String code() { return "counter.negative"; }
        @Override public String message() { return "cannot decrement " + value + " by " + attempted; }
    }

    @Override public String aggregateType() { return "Counter"; }

    @Override public Counter initial() { return new Counter(0); }

    @Override public Counter evolve(Counter state, CounterEvent event) {
        return switch (event) {
            case Incremented e -> new Counter(state.value() + e.by());
            case Decremented e -> new Counter(state.value() - e.by());
        };
    }

    @Override public Either<DomainError, List<CounterEvent>> decide(Counter state, CounterCommand command) {
        return switch (command) {
            case Increment c -> Either.right(List.of(new Incremented(c.by())));
            case Decrement c -> state.value() - c.by() < 0
                    ? Either.left(new NegativeCounter(state.value(), c.by()))
                    : Either.right(List.of(new Decremented(c.by())));
        };
    }
}
