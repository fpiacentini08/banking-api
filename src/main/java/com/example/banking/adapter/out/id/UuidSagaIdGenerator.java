package com.example.banking.adapter.out.id;

import com.example.banking.eventsourcing.saga.SagaIdGenerator;

import java.util.UUID;

/** Bean-defined in {@code EventSourcingConfig}, like the kernel clock: the kernel stays free of
 *  Spring annotations. */
public class UuidSagaIdGenerator implements SagaIdGenerator {

    @Override
    public String next() {
        return UUID.randomUUID().toString();
    }
}
