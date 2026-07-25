package com.example.banking.application;

import java.util.Optional;

/** Read port over the account_balance projection. */
public interface AccountBalances {
    Optional<AccountBalanceView> find(String accountId);
}
