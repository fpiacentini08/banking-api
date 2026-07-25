package com.example.banking.application;

import io.vavr.control.Either;
import org.springframework.stereotype.Component;

/** Query service for the transaction-status resource. */
@Component
public class TransactionStatusQuery {

    private final TransactionStatusStore store;

    public TransactionStatusQuery(TransactionStatusStore store) {
        this.store = store;
    }

    public Either<ApplicationError, TransactionStatus> status(String transactionId) {
        return store.find(transactionId)
                .map(status -> Either.<ApplicationError, TransactionStatus>right(status))
                .orElseGet(() -> Either.left(new ApplicationError.TransactionNotFound(transactionId)));
    }
}
