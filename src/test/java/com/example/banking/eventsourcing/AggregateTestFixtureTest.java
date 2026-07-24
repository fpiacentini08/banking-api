package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.fixture.AggregateTestFixture;
import com.example.banking.eventsourcing.support.CounterBehaviour;
import org.junit.jupiter.api.Test;

import static com.example.banking.eventsourcing.support.CounterBehaviour.*;

class AggregateTestFixtureTest {

    private final AggregateTestFixture<Counter, CounterCommand, CounterEvent> fixture =
            AggregateTestFixture.forBehaviour(new CounterBehaviour());

    @Test
    void rightArm_incrementEmitsIncremented() {
        fixture.given(new Incremented(2))
                .when(new Increment("c-1", 3))
                .expectEvents(new Incremented(3));
    }

    @Test
    void leftArm_decrementBelowZeroIsRejected() {
        fixture.given(new Incremented(1))
                .when(new Decrement("c-1", 5))
                .expectError(new NegativeCounter(1, 5));
    }
}
