package com.example.banking.adapter.in.web;

import com.example.banking.application.TransactionStatusQuery;
import com.example.banking.domain.shared.TransactionId;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class TransactionStatusController {

    private final TransactionStatusQuery query;

    public TransactionStatusController(TransactionStatusQuery query) {
        this.query = query;
    }

    @GetMapping("/transactions/{transactionId}")
    public TransactionStatusResponse status(@PathVariable String transactionId) {
        return query.status(new TransactionId(transactionId)).fold(
                error -> { throw WebErrors.toResponseStatus(error); },
                TransactionStatusResponse::from);
    }
}
