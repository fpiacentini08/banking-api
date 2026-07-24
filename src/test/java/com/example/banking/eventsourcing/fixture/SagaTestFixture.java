package com.example.banking.eventsourcing.fixture;

import com.example.banking.eventsourcing.saga.DeadlineRequest;
import com.example.banking.eventsourcing.saga.SagaBehaviour;
import com.example.banking.eventsourcing.saga.SagaUpdate;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure given/when/then for sagas: folds given events through react, asserts the last update. */
public final class SagaTestFixture<S> {

    private final SagaBehaviour<S> behaviour;
    private S state;
    private boolean started;

    private SagaTestFixture(SagaBehaviour<S> behaviour) {
        this.behaviour = behaviour;
    }

    public static <S> SagaTestFixture<S> forBehaviour(SagaBehaviour<S> behaviour) {
        return new SagaTestFixture<>(behaviour);
    }

    public SagaTestFixture<S> givenNoPriorActivity() {
        return this;
    }

    public SagaTestFixture<S> given(Object... events) {
        for (Object event : events) {
            apply(event);
        }
        return this;
    }

    public Then whenEvent(Object event) {
        return new Then(apply(event));
    }

    private SagaUpdate<S> apply(Object event) {
        String key = behaviour.associationKey(event)
                .orElseThrow(() -> new AssertionError("event has no association key: " + event));
        if (!started) {
            assertThat(behaviour.startsSaga(event))
                    .as("first event must start the saga: %s", event).isTrue();
            state = behaviour.initial(key);
            started = true;
        }
        SagaUpdate<S> update = behaviour.react(state, event);
        state = update.state();
        return update;
    }

    public final class Then {
        private final SagaUpdate<S> update;

        private Then(SagaUpdate<S> update) {
            this.update = update;
        }

        public Then expectDispatched(Object... commands) {
            assertThat(update.commands()).containsExactly(commands);
            return this;
        }

        public Then expectNoCommands() {
            assertThat(update.commands()).isEmpty();
            return this;
        }

        public Then expectScheduled(String... deadlineIds) {
            assertThat(update.schedule()).extracting(DeadlineRequest::deadlineId)
                    .containsExactly(deadlineIds);
            return this;
        }

        public Then expectCancelled(String... deadlineIds) {
            assertThat(update.cancelDeadlines()).containsExactly(deadlineIds);
            return this;
        }

        public Then expectTerminal() {
            assertThat(update.terminal()).as("saga should be terminal").isTrue();
            return this;
        }
    }
}
