package com.example.banking.eventsourcing.command;

import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.common.DomainError;

import io.vavr.control.Either;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public final class StripedCommandBus implements CommandBus, AutoCloseable {

    private record Registration<C>(Function<C, String> aggregateIdOf, CommandHandler<C> handler) {}

    private final ExecutorService[] stripes;
    private final Map<Class<?>, Registration<?>> registrations = new ConcurrentHashMap<>();

    public StripedCommandBus(int stripeCount) {
        this.stripes = new ExecutorService[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public <C> void register(Class<C> commandType, Function<C, String> aggregateIdOf, CommandHandler<C> handler) {
        registrations.put(commandType, new Registration<>(aggregateIdOf, handler));
    }

    @Override
    public <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command) {
        @SuppressWarnings("unchecked")
        Registration<C> registration = (Registration<C>) registrations.get(command.getClass());
        if (registration == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("no handler registered for " + command.getClass().getName()));
        }
        String aggregateId = registration.aggregateIdOf().apply(command);
        ExecutorService stripe = stripes[Math.floorMod(aggregateId.hashCode(), stripes.length)];
        return CompletableFuture.supplyAsync(() -> registration.handler().handle(command), stripe);
    }

    @Override
    public void close() {
        for (ExecutorService stripe : stripes) {
            stripe.shutdown();
        }
    }
}
