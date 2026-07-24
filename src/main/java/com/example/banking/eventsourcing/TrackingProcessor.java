package com.example.banking.eventsourcing;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Named poll loop over the event store. Handler effects and the token update run inside one
 * transaction, so MySQL-backed handlers get exactly-once semantics; handlers with external
 * effects must be idempotent (at-least-once).
 */
public final class TrackingProcessor {

    private final String name;
    private final EventStore eventStore;
    private final TokenStore tokenStore;
    private final TransactionalRunner tx;
    private final EventHandler handler;
    private final int batchSize;
    private final Duration pollInterval;
    private volatile boolean running;
    private Thread loop;

    public TrackingProcessor(String name, EventStore eventStore, TokenStore tokenStore,
                             TransactionalRunner tx, EventHandler handler,
                             int batchSize, Duration pollInterval) {
        this.name = name;
        this.eventStore = eventStore;
        this.tokenStore = tokenStore;
        this.tx = tx;
        this.handler = handler;
        this.batchSize = batchSize;
        this.pollInterval = pollInterval;
    }

    public String name() {
        return name;
    }

    /** One transactional batch: read after token, handle, advance token. Returns events applied. */
    public int processOnce() {
        AtomicInteger applied = new AtomicInteger();
        tx.inTransaction(() -> {
            long token = tokenStore.load(name);
            List<StoredEvent> batch = eventStore.readAllAfter(token, batchSize);
            for (StoredEvent event : batch) {
                handler.handle(event);
            }
            if (!batch.isEmpty()) {
                tokenStore.save(name, batch.get(batch.size() - 1).globalPosition());
            }
            applied.set(batch.size());
        });
        return applied.get();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        loop = new Thread(this::runLoop, "tracking-processor-" + name);
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
            int applied;
            try {
                applied = processOnce();
            } catch (RuntimeException e) {
                applied = 0;  // roll back and retry after the poll interval; the token did not move
            }
            if (applied < batchSize) {
                try {
                    Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
