package com.example.banking.domain.user;

public sealed interface UserCommand permits RegisterUser {
    UserId userId();
}
