package com.example.banking.adapter.in.web;

public record TransactionStatusResponse(String transactionId, String status,
                                        String resultUserId, String reason) {}
