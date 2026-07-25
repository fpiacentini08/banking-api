package com.example.banking.domain.account;

/** Typed account identifier. The value is opaque; generation lives in the application layer. */
public record AccountId(String value) {
    public AccountId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("accountId must not be blank");
        }
    }
}
