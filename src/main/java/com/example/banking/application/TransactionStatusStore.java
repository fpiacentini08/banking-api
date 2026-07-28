package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;

import java.util.Optional;

/** Out-port for the transaction-status resource: one PENDING insert on accept, a monotonic
 *  terminal transition on outcome, and a read for the status endpoint. */
public interface TransactionStatusStore {
    void insertPending(TransactionId transactionId, String type);
    void markCompleted(TransactionId transactionId, String resultUserId);
    void markRejected(TransactionId transactionId, String reason);
    void markFailed(TransactionId transactionId, String reason);
    Optional<TransactionStatus> find(TransactionId transactionId);
}
