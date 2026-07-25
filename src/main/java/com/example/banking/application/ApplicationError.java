package com.example.banking.application;

/** A synchronous request-time failure, carried in the left arm of an application-service Either.
 *  Distinct from the kernel's async {@code DomainError}. */
public sealed interface ApplicationError {

    String message();

    record UserNotFound(String userId) implements ApplicationError {
        @Override public String message() { return "user not found: " + userId; }
    }

    record AccountNotFound(String accountId) implements ApplicationError {
        @Override public String message() { return "account not found: " + accountId; }
    }

    record NotAccountOwner(String accountId) implements ApplicationError {
        @Override public String message() { return "not the account owner"; }
    }

    record TransactionNotFound(String transactionId) implements ApplicationError {
        @Override public String message() { return "unknown transaction " + transactionId; }
    }
}
