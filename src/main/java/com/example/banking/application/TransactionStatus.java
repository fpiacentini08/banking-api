package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;

/** A transaction's current status. {@code resultUserId}/{@code reason} are null until set. */
public record TransactionStatus(TransactionId transactionId, String type, TransactionState state,
                                String resultUserId, String reason) {}
