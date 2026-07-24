package com.example.banking.adapter.out.eventstore.store;

import com.example.banking.eventsourcing.common.TransactionalRunner;
import org.springframework.transaction.support.TransactionTemplate;

public final class SpringTransactionalRunner implements TransactionalRunner {

    private final TransactionTemplate transactionTemplate;

    public SpringTransactionalRunner(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public void inTransaction(Runnable work) {
        transactionTemplate.executeWithoutResult(status -> work.run());
    }
}
