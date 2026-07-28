package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.aggregate.AggregateBehaviour;
import com.example.banking.eventsourcing.common.DomainError;
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

    /** Test serializer: encodes counter events as "<SimpleName>:<by>". */
    public static com.example.banking.eventsourcing.event.EventSerializer testSerializer() {
        SequentialIds eventIds = new SequentialIds("counter-event");
        return new com.example.banking.eventsourcing.event.EventSerializer() {
            @Override
            public com.example.banking.eventsourcing.event.SerializedEvent serialize(Object event, java.util.Map<String, String> metadata) {
                int by = event instanceof Incremented i ? i.by() : ((Decremented) event).by();
                return new com.example.banking.eventsourcing.event.SerializedEvent(
                        eventIds.next(),
                        event.getClass().getSimpleName(), 1,
                        event.getClass().getSimpleName() + ":" + by, "{}",
                        java.time.Instant.EPOCH);
            }

            @Override
            public Object deserialize(com.example.banking.eventsourcing.event.SerializedEvent event) {
                int by = Integer.parseInt(event.payload().split(":")[1]);
                return event.payload().startsWith("Incremented") ? new Incremented(by) : new Decremented(by);
            }
        };
    }

    /** Test codec for Counter state: encodes the int value as a string. */
    public static com.example.banking.eventsourcing.common.PayloadCodec testCodec() {
        return new com.example.banking.eventsourcing.common.PayloadCodec() {
            @Override public String encode(Object value) { return String.valueOf(((Counter) value).value()); }
            @Override @SuppressWarnings("unchecked")
            public <T> T decode(String json, Class<T> type) { return (T) new Counter(Integer.parseInt(json)); }
        };
    }
}
