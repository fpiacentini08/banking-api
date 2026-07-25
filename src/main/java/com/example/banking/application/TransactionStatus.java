package com.example.banking.application;

/** A transaction's current status. {@code resultUserId}/{@code reason} are null until set. */
public record TransactionStatus(String transactionId, String type, TransactionState state,
                                String resultUserId, String reason) {}
