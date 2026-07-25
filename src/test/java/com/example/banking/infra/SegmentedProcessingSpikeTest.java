package com.example.banking.infra;

import com.example.banking.adapter.out.eventstore.store.JooqEventStore;
import com.example.banking.adapter.out.eventstore.processor.SegmentedEventProcessor;
import com.example.banking.adapter.out.eventstore.store.SpringTransactionalRunner;
import com.example.banking.eventsourcing.processor.EventHandler;
import com.example.banking.eventsourcing.event.EventStore;
import com.example.banking.eventsourcing.event.SerializedEvent;
import com.example.banking.eventsourcing.common.TransactionalRunner;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPIKE — proves the SKIP LOCKED scale-out for the read side. Simulates several pods as worker
 * threads, all draining the same segmented processor against one MySQL. Asserts: every event is
 * processed exactly once (no double-processing), per-aggregate order is preserved despite parallel
 * segments, and more than one worker actually did work (real parallelism, not single-active).
 */
@SpringBootTest
@Import(ContainersConfig.class)
class SegmentedProcessingSpikeTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM domain_event");
        jdbc.update("DELETE FROM segment_token");
        jdbc.update("UPDATE event_store_sequence SET next_position = 1");
    }

    private static SerializedEvent event() {
        return new SerializedEvent(UUID.randomUUID().toString(), "Ev", 1, "{}", "{}", Instant.now());
    }

    @Test
    void concurrentWorkersProcessEveryEventOnceInPerAggregateOrderAcrossThreads() throws Exception {
        TransactionalRunner tx = new SpringTransactionalRunner(new TransactionTemplate(txManager));
        EventStore store = new JooqEventStore(dsl, tx);

        int aggregates = 40;
        int perAggregate = 5;
        int total = aggregates * perAggregate;
        // Append round-robin so the global stream interleaves aggregates (seq r for every aggregate,
        // round by round). For any single aggregate, later seq => later global_position.
        for (int r = 0; r < perAggregate; r++) {
            for (int a = 0; a < aggregates; a++) {
                store.append("Acct", "acct-" + a, r - 1, List.of(event()));
            }
        }

        ConcurrentMap<String, List<Long>> orderPerAggregate = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<Long> allPositions = new ConcurrentLinkedQueue<>();
        java.util.Set<String> workerThreads = ConcurrentHashMap.newKeySet();
        AtomicInteger processed = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();

        EventHandler handler = e -> {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                workerThreads.add(Thread.currentThread().getName());
                orderPerAggregate.computeIfAbsent(e.aggregateId(), k -> new CopyOnWriteArrayList<>())
                        .add(e.sequenceNr());
                allPositions.add(e.globalPosition());
                processed.incrementAndGet();
                Thread.sleep(10);  // widen the window so genuine parallelism is observable
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
        };

        int workers = 4;
        List<SegmentedEventProcessor> procs = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            procs.add(new SegmentedEventProcessor(jdbc, tx, "spike-proj", 10, handler));
        }
        procs.get(0).ensureSegments();

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        List<Future<?>> futures = new ArrayList<>();
        for (SegmentedEventProcessor proc : procs) {
            futures.add(pool.submit(() -> {
                while (processed.get() < total && System.nanoTime() < deadlineNanos) {
                    int n = proc.processOneClaim();
                    if (n == 0) {
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(40, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        assertThat(processed.get()).as("all events processed").isEqualTo(total);
        assertThat(allPositions).as("each event handled").hasSize(total);
        assertThat(new HashSet<>(allPositions)).as("no double-processing").hasSize(total);
        assertThat(orderPerAggregate).as("every aggregate seen").hasSize(aggregates);
        orderPerAggregate.forEach((aggregate, seqs) ->
                assertThat(seqs).as("per-aggregate order preserved for %s", aggregate).isSorted());
        assertThat(workerThreads).as("more than one worker thread did processing").hasSizeGreaterThan(1);
        assertThat(maxInFlight.get())
                .as("two workers processed different segments at the same instant (real parallelism)")
                .isGreaterThanOrEqualTo(2);
    }
}
