package com.example.banking.eventsourcing.command;

import com.example.banking.eventsourcing.aggregate.CommittedEvents;
import com.example.banking.eventsourcing.common.DomainError;

import io.vavr.control.Either;

public interface CommandHandler<C> {
    Either<DomainError, CommittedEvents> handle(C command);
}
