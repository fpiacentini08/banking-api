package com.example.banking.eventsourcing;

import com.example.banking.eventsourcing.fixture.SagaTestFixture;
import com.example.banking.eventsourcing.support.TransferLikeSaga;
import org.junit.jupiter.api.Test;

import static com.example.banking.eventsourcing.support.TransferLikeSaga.*;

class SagaTestFixtureTest {

    private final SagaTestFixture<TransferLikeSaga.State> fixture =
            SagaTestFixture.forBehaviour(new TransferLikeSaga());

    @Test
    void happyPath_creditAfterDebit() {
        fixture.given(new Requested("t-1"), new Debited("t-1"))
                .whenEvent(new Credited("t-1"))
                .expectNoCommands()
                .expectCancelled("timeout-t-1")
                .expectTerminal();
    }

    @Test
    void compensation_refundOnCreditFailure() {
        fixture.given(new Requested("t-2"), new Debited("t-2"))
                .whenEvent(new CreditFailed("t-2"))
                .expectDispatched(new RefundCmd("t-2"))
                .expectTerminal();
    }

    @Test
    void start_schedulesTimeoutDeadline() {
        fixture.givenNoPriorActivity()
                .whenEvent(new Requested("t-3"))
                .expectDispatched(new DebitCmd("t-3"))
                .expectScheduled("timeout-t-3");
    }
}
