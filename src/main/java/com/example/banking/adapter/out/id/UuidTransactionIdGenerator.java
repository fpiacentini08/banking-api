package com.example.banking.adapter.out.id;

import com.example.banking.application.TransactionIdGenerator;
import com.example.banking.domain.shared.TransactionId;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UuidTransactionIdGenerator implements TransactionIdGenerator {

    @Override
    public TransactionId next() {
        return new TransactionId(UUID.randomUUID().toString());
    }
}
