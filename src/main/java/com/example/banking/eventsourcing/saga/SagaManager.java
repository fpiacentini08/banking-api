package com.example.banking.eventsourcing.saga;

import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.common.PayloadCodec;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.processor.EventHandler;

/**
 * Runs a saga behaviour as an event handler on a tracking processor: correlate, load or start,
 * react, persist, then perform effects (dispatch commands, schedule/cancel deadlines).
 * Effects are at-least-once on redelivery; commands must be idempotent downstream.
 */
public final class SagaManager<S> implements EventHandler {

    private final SagaBehaviour<S> behaviour;
    private final SagaStore store;
    private final PayloadCodec codec;
    private final Class<S> stateType;
    private final EventSerializer serializer;
    private final CommandBus commandBus;
    private final DeadlineScheduler deadlines;
    private final SagaIdGenerator sagaIds;

    public SagaManager(SagaBehaviour<S> behaviour, SagaStore store, PayloadCodec codec,
                       Class<S> stateType, EventSerializer serializer,
                       CommandBus commandBus, DeadlineScheduler deadlines,
                       SagaIdGenerator sagaIds) {
        this.behaviour = behaviour;
        this.store = store;
        this.codec = codec;
        this.stateType = stateType;
        this.serializer = serializer;
        this.commandBus = commandBus;
        this.deadlines = deadlines;
        this.sagaIds = sagaIds;
    }

    @Override
    public void handle(StoredEvent stored) {
        Object event = serializer.deserialize(stored.event());
        behaviour.associationKey(event).ifPresent(key -> handleCorrelated(key, event));
    }

    /** Deadline delivery path: the poller knows the saga id directly, no association lookup. */
    public void handleDeadline(String sagaId, Object deadlineEvent) {
        store.findById(sagaId)
                .filter(instance -> !instance.terminal())
                .ifPresent(instance -> reactAndPersist(instance, deadlineEvent));
    }

    private void handleCorrelated(String key, Object event) {
        SagaInstance instance = store.findByAssociation(behaviour.sagaType(), key).orElse(null);
        if (instance == null) {
            if (!behaviour.startsSaga(event)) {
                return;
            }
            instance = new SagaInstance(sagaIds.next(), behaviour.sagaType(),
                    codec.encode(behaviour.initial(key)), false);
            store.insert(instance, key);
        }
        if (instance.terminal()) {
            return;
        }
        reactAndPersist(instance, event);
    }

    private void reactAndPersist(SagaInstance instance, Object event) {
        S state = codec.decode(instance.statePayload(), stateType);
        SagaUpdate<S> update = behaviour.react(state, event);
        store.save(new SagaInstance(instance.sagaId(), instance.sagaType(),
                codec.encode(update.state()), update.terminal()));
        update.cancelDeadlines().forEach(deadlines::cancel);
        update.schedule().forEach(request ->
                deadlines.schedule(behaviour.sagaType(), instance.sagaId(), request));
        update.commands().forEach(commandBus::dispatch);
    }
}
