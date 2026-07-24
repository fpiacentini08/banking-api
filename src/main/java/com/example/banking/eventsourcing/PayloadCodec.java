package com.example.banking.eventsourcing;

/** JSON codec port used for snapshots, saga state, and deadline payloads. */
public interface PayloadCodec {
    String encode(Object value);
    <T> T decode(String json, Class<T> type);
}
