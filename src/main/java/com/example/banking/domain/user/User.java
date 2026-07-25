package com.example.banking.domain.user;

/** Aggregate state. {@code registered} is false until a {@link UserRegistered} event is applied. */
public record User(boolean registered, UserId userId, String name, String email) {}
