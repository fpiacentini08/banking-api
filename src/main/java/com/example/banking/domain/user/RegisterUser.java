package com.example.banking.domain.user;

public record RegisterUser(UserId userId, String name, String email) implements UserCommand {}
