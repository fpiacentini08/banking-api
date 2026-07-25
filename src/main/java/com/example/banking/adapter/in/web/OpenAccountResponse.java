package com.example.banking.adapter.in.web;

import com.example.banking.application.AccountOpeningAccepted;

public record OpenAccountResponse(String accountId, String status, String statusUrl) {
    static OpenAccountResponse from(AccountOpeningAccepted accepted) {
        return new OpenAccountResponse(accepted.accountId(), "PENDING",
                "/transactions/" + accepted.transactionId());
    }
}
