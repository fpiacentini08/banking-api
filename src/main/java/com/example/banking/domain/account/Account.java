package com.example.banking.domain.account;

import com.example.banking.domain.user.UserId;

/** Aggregate state. {@code opened} is false until an {@link AccountOpened} event is applied.
 *  Balance is integer minor units (cents), EUR fixed; it stays 0 until deposits (a later milestone). */
public record Account(boolean opened, AccountId accountId, UserId ownerId, long balanceMinor) {}
