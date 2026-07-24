package com.example.banking.eventsourcing;

/** A business-rule failure. Never thrown — always carried in the left arm of an Either. */
public interface DomainError {
    String code();
    String message();
}
