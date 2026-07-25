package com.example.banking.domain.account;

import com.example.banking.eventsourcing.common.DomainError;

public record AccountAlreadyOpened(AccountId accountId) implements DomainError {
    @Override public String code() { return "account.already-opened"; }
    @Override public String message() { return "account " + accountId.value() + " is already opened"; }
}
