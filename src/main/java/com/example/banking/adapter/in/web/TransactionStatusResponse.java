package com.example.banking.adapter.in.web;

import com.example.banking.application.TransactionStatus;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionStatusResponse(String transactionId, String status, String resultUserId, String reason) {
    static TransactionStatusResponse from(TransactionStatus status) {
        return new TransactionStatusResponse(status.transactionId().value(), status.state().name(),
                status.resultUserId(), status.reason());
    }
}
