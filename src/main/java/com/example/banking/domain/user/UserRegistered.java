package com.example.banking.domain.user;

public record UserRegistered(UserId userId, String name, String email) implements UserEvent {}
