package com.example.banking.adapter.in.web;

import com.example.banking.application.TransactionAccepted;

public record RegisterUserResponse(String transactionId, String status, String statusUrl) {
    static RegisterUserResponse from(TransactionAccepted accepted) {
        return new RegisterUserResponse(accepted.transactionId(), "PENDING",
                "/transactions/" + accepted.transactionId());
    }
}
