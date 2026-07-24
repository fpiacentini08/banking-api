package com.example.banking.eventsourcing.command;

import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.common.DomainError;

import io.vavr.control.Either;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Asynchronous in-process command dispatch. Registration is explicit; the aggregateIdOf function
 * routes each command to a stripe so same-aggregate commands execute serially in dispatch order.
 */
public interface CommandBus {

    <C> void register(Class<C> commandType, Function<C, String> aggregateIdOf, CommandHandler<C> handler);

    <C> CompletableFuture<Either<DomainError, CommittedEvents>> dispatch(C command);
}
