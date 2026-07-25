package com.example.banking.adapter.out.eventstore.saga;

import com.example.banking.adapter.out.eventstore.saga.DeadlineRepository.DueDeadline;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import com.example.banking.eventsourcing.common.PayloadCodec;
import com.example.banking.eventsourcing.saga.SagaManager;
import com.example.banking.eventsourcing.common.TransactionalRunner;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Delivers due deadlines to their saga manager and deletes them, one transaction per poll. */
public final class DeadlinePoller {

    private final DeadlineRepository deadlines;
    private final TransactionalRunner tx;
    private final PayloadCodec codec;
    private final EventTypeRegistry registry;
    private final Map<String, SagaManager<?>> managersBySagaType;
    private final Clock clock;
    private final Duration pollInterval;
    private volatile boolean running;
    private Thread loop;

    public DeadlinePoller(DeadlineRepository deadlines, TransactionalRunner tx, PayloadCodec codec,
                          EventTypeRegistry registry, Map<String, SagaManager<?>> managersBySagaType,
                          Clock clock, Duration pollInterval) {
        this.deadlines = deadlines;
        this.tx = tx;
        this.codec = codec;
        this.registry = registry;
        this.managersBySagaType = Map.copyOf(managersBySagaType);
        this.clock = clock;
        this.pollInterval = pollInterval;
    }

    /** One transactional pass: deliver every due deadline, delete it, return the count. */
    public int pollOnce() {
        AtomicInteger delivered = new AtomicInteger();
        tx.inTransaction(() -> {
            for (DueDeadline deadline : deadlines.findDue(clock.instant(), 100)) {
                SagaManager<?> manager = managersBySagaType.get(deadline.sagaType());
                if (manager != null) {
                    Object payload = codec.decode(deadline.payload(),
                            registry.byName(deadline.payloadType()).type());
                    manager.handleDeadline(deadline.sagaId(), payload);
                }
                deadlines.delete(deadline.deadlineId());
                delivered.incrementAndGet();
            }
        });
        return delivered.get();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = new Thread(this::runLoop, "deadline-poller");
        loop.setDaemon(true);
        loop.start();
    }

    public synchronized void stop() {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    private void runLoop() {
        while (running) {
            try {
                pollOnce();
            } catch (RuntimeException e) {
                // roll back; due deadlines stay in the table and are retried next pass
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
