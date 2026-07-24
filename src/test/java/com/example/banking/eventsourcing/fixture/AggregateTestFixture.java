package com.example.banking.eventsourcing.fixture;

import com.example.banking.eventsourcing.aggregate.AggregateBehaviour;
import com.example.banking.eventsourcing.common.DomainError;
import io.vavr.control.Either;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure given/when/then fixture: folds given events with evolve, runs decide, asserts either arm. */
public final class AggregateTestFixture<S, C, E> {

    private final AggregateBehaviour<S, C, E> behaviour;
    private S state;

    private AggregateTestFixture(AggregateBehaviour<S, C, E> behaviour) {
        this.behaviour = behaviour;
        this.state = behaviour.initial();
    }

    public static <S, C, E> AggregateTestFixture<S, C, E> forBehaviour(AggregateBehaviour<S, C, E> behaviour) {
        return new AggregateTestFixture<>(behaviour);
    }

    @SafeVarargs
    public final AggregateTestFixture<S, C, E> given(E... events) {
        for (E event : events) {
            state = behaviour.evolve(state, event);
        }
        return this;
    }

    public When when(C command) {
        return new When(behaviour.decide(state, command));
    }

    public final class When {
        private final Either<DomainError, List<E>> outcome;

        private When(Either<DomainError, List<E>> outcome) {
            this.outcome = outcome;
        }

        @SafeVarargs
        public final void expectEvents(E... expected) {
            assertThat(outcome.isRight())
                    .as("expected events %s but got error %s", List.of(expected), outcome.swap().getOrNull())
                    .isTrue();
            assertThat(outcome.get()).containsExactly(expected);
        }

        public void expectError(DomainError expected) {
            assertThat(outcome.isLeft())
                    .as("expected error %s but got events %s", expected, outcome.getOrNull())
                    .isTrue();
            assertThat(outcome.getLeft()).isEqualTo(expected);
        }
    }
}
