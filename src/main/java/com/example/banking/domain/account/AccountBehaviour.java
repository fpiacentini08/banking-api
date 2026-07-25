package com.example.banking.domain.account;

import com.example.banking.eventsourcing.aggregate.AggregateBehaviour;
import com.example.banking.eventsourcing.common.DomainError;
import io.vavr.control.Either;

import java.util.List;

/** Pure Account aggregate: an account may be opened exactly once. */
public final class AccountBehaviour implements AggregateBehaviour<Account, AccountCommand, AccountEvent> {

    @Override public String aggregateType() { return "Account"; }

    @Override public Account initial() { return new Account(false, null, null, 0); }

    @Override public Account evolve(Account state, AccountEvent event) {
        return switch (event) {
            case AccountOpened e -> new Account(true, e.accountId(), e.ownerId(), 0);
        };
    }

    @Override public Either<DomainError, List<AccountEvent>> decide(Account state, AccountCommand command) {
        return switch (command) {
            case OpenAccount c -> state.opened()
                    ? Either.left(new AccountAlreadyOpened(c.accountId()))
                    : Either.right(List.of(new AccountOpened(c.accountId(), c.ownerId())));
        };
    }
}
