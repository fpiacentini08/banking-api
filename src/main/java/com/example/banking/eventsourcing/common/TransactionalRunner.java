package com.example.banking.eventsourcing.common;

/** Port over the platform transaction manager so the kernel stays Spring-free. */
public interface TransactionalRunner {
    void inTransaction(Runnable work);
}
