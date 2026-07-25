package com.example.banking.domain.account;

public sealed interface AccountCommand permits OpenAccount {
    AccountId accountId();
}
