package com.example.banking.eventsourcing.processor;

/** Persisted per-processor progress through the global event stream. */
public interface TokenStore {

    /** Last applied global position; 0 for a processor that has never run. */
    long load(String processorName);

    void save(String processorName, long position);

    /** Rebuild support: next load returns 0 and the processor replays from the start. */
    void reset(String processorName);
}
