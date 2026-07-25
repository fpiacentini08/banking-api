package com.example.banking.domain.user;

import com.example.banking.eventsourcing.common.DomainError;

public record UserAlreadyRegistered(UserId userId) implements DomainError {
    @Override public String code() { return "user.already-registered"; }
    @Override public String message() { return "user " + userId.value() + " is already registered"; }
}
