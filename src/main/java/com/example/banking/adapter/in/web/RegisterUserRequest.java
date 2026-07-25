package com.example.banking.adapter.in.web;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record RegisterUserRequest(@NotBlank String name, @NotBlank @Email String email) {}
