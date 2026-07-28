package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;
import io.vavr.control.Either;
import org.springframework.stereotype.Component;

/** Query service for the transaction-status resource. */
@Component
public class TransactionStatusQuery {

    private final TransactionStatusStore store;

    public TransactionStatusQuery(TransactionStatusStore store) {
        this.store = store;
    }

    public Either<ApplicationError, TransactionStatus> status(TransactionId transactionId) {
        return store.find(transactionId)
                .map(status -> Either.<ApplicationError, TransactionStatus>right(status))
                .orElseGet(() -> Either.left(new ApplicationError.TransactionNotFound(transactionId)));
    }
}
