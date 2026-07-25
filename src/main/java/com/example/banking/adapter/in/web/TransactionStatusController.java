package com.example.banking.adapter.in.web;

import com.example.banking.application.TransactionStatus;
import com.example.banking.application.TransactionStatusStore;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public final class TransactionStatusController {

    private final TransactionStatusStore store;

    public TransactionStatusController(TransactionStatusStore store) {
        this.store = store;
    }

    @GetMapping("/transactions/{transactionId}")
    public TransactionStatusResponse status(@PathVariable String transactionId) {
        TransactionStatus found = store.find(transactionId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "unknown transaction " + transactionId));
        return new TransactionStatusResponse(
                found.transactionId(), found.state().name(), found.resultUserId(), found.reason());
    }
}
