package com.example.banking.adapter.in.web;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransactionStatusResponse(String transactionId, String status,
                                        String resultUserId, String reason) {}
