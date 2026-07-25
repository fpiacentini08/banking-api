package com.example.banking.adapter.in.web;

import com.example.banking.application.AccountBalanceReadModel;

import java.time.Instant;

public record BalanceResponse(String balance, String currency, Instant asOf) {
    static BalanceResponse from(AccountBalanceReadModel model) {
        return new BalanceResponse(model.balance(), model.currency(), model.asOf());
    }
}
