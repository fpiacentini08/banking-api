package com.example.banking.eventsourcing;

import io.vavr.control.Either;

public interface CommandHandler<C> {
    Either<DomainError, CommittedEvents> handle(C command);
}
