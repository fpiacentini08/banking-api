package com.example.banking.adapter.in.web;

import jakarta.validation.constraints.NotBlank;

public record OpenAccountRequest(@NotBlank String userId) {}
