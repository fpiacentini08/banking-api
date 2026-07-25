package com.example.banking.domain.user;

import com.example.banking.eventsourcing.aggregate.AggregateBehaviour;
import com.example.banking.eventsourcing.common.DomainError;
import io.vavr.control.Either;

import java.util.List;

/** Pure User aggregate: a user may be registered exactly once. */
public final class UserBehaviour implements AggregateBehaviour<User, UserCommand, UserEvent> {

    @Override public String aggregateType() { return "User"; }

    @Override public User initial() { return new User(false, null, null, null); }

    @Override public User evolve(User state, UserEvent event) {
        return switch (event) {
            case UserRegistered e -> new User(true, e.userId(), e.name(), e.email());
        };
    }

    @Override public Either<DomainError, List<UserEvent>> decide(User state, UserCommand command) {
        return switch (command) {
            case RegisterUser c -> state.registered()
                    ? Either.left(new UserAlreadyRegistered(c.userId()))
                    : Either.right(List.of(new UserRegistered(c.userId(), c.name(), c.email())));
        };
    }
}
