package com.example.banking.domain.account;

import com.example.banking.domain.user.UserId;

public record AccountOpened(AccountId accountId, UserId ownerId) implements AccountEvent {}
