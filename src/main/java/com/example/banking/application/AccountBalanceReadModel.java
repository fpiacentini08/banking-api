package com.example.banking.application;

import java.time.Instant;

/** The rendered account balance: money as a decimal string, EUR fixed. */
public record AccountBalanceReadModel(String balance, String currency, Instant asOf) {}
