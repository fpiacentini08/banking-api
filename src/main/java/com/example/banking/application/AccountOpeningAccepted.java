package com.example.banking.application;

import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.shared.TransactionId;

public record AccountOpeningAccepted(AccountId accountId, TransactionId transactionId) {}
