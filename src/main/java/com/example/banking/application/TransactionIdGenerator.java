package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;

/** Out-port for minting transaction identifiers. A later milestone derives the value from the
 *  client {@code Idempotency-Key} instead of generating one. */
public interface TransactionIdGenerator {
    TransactionId next();
}
