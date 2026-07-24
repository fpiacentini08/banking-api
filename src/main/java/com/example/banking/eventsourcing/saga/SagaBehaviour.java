package com.example.banking.eventsourcing.saga;

import java.util.Optional;

/** Pure saga state machine: correlate events, react with new state and effects. */
public interface SagaBehaviour<S> {

    String sagaType();

    /** The correlation value for this event, or empty when the event is not this saga's concern. */
    Optional<String> associationKey(Object event);

    boolean startsSaga(Object event);

    S initial(String associationKey);

    SagaUpdate<S> react(S state, Object event);
}
