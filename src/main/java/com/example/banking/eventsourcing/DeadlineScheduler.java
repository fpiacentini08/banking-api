package com.example.banking.eventsourcing;

/** Port for saga timeouts. The JDBC implementation and its poller live in the adapter. */
public interface DeadlineScheduler {

    void schedule(String sagaType, String sagaId, DeadlineRequest request);

    void cancel(String deadlineId);
}
