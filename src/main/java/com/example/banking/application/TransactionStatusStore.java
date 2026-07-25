package com.example.banking.application;

import java.util.Optional;

/** Out-port for the transaction-status resource: one PENDING insert on accept, a monotonic
 *  terminal transition on outcome, and a read for the status endpoint. */
public interface TransactionStatusStore {
    void insertPending(String transactionId, String type);
    void markCompleted(String transactionId, String resultUserId);
    void markRejected(String transactionId, String reason);
    void markFailed(String transactionId, String reason);
    Optional<TransactionStatus> find(String transactionId);
}
