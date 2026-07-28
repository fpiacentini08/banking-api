package com.example.banking.domain.shared;

/** Typed transaction identifier. The value is opaque; generation lives in the application layer. */
public record TransactionId(String value) {
    public TransactionId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("transactionId must not be blank");
        }
    }
}
