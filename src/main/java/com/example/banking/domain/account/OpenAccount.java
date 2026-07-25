package com.example.banking.domain.account;

import com.example.banking.domain.user.UserId;

public record OpenAccount(AccountId accountId, UserId ownerId) implements AccountCommand {}
