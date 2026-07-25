package com.example.banking.adapter.in.web;

import java.time.Instant;

public record BalanceResponse(String balance, String currency, Instant asOf) {}
