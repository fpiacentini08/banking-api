package com.example.banking.application;

import java.time.Instant;

public record AccountBalanceView(String ownerId, long balanceMinor, long version, Instant updatedAt) {}
