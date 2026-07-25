package com.example.banking.domain.user;

/** Typed user identifier. The value is opaque; generation lives in the application layer. */
public record UserId(String value) {
    public UserId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("userId must not be blank");
        }
    }
}
