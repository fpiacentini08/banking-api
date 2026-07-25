package com.example.banking.config;

import com.example.banking.eventsourcing.processor.TrackingProcessor;
import org.springframework.context.SmartLifecycle;

/** Starts and stops a TrackingProcessor's background poll loop with the Spring context. */
public final class TrackingProcessorLifecycle implements SmartLifecycle {

    private final TrackingProcessor processor;
    private volatile boolean running;

    public TrackingProcessorLifecycle(TrackingProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void start() {
        processor.start();
        running = true;
    }

    @Override
    public void stop() {
        processor.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
