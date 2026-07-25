package com.example.banking.domain.account;

import com.example.banking.domain.user.UserId;
import com.example.banking.eventsourcing.fixture.AggregateTestFixture;
import org.junit.jupiter.api.Test;

class AccountBehaviourTest {

    private final AggregateTestFixture<Account, AccountCommand, AccountEvent> fixture =
            AggregateTestFixture.forBehaviour(new AccountBehaviour());

    @Test
    void rightArm_openEmitsAccountOpened() {
        AccountId accountId = new AccountId("a-1");
        UserId ownerId = new UserId("u-1");
        fixture.given()
                .when(new OpenAccount(accountId, ownerId))
                .expectEvents(new AccountOpened(accountId, ownerId));
    }

    @Test
    void leftArm_reopenIsRejected() {
        AccountId accountId = new AccountId("a-1");
        UserId ownerId = new UserId("u-1");
        fixture.given(new AccountOpened(accountId, ownerId))
                .when(new OpenAccount(accountId, ownerId))
                .expectError(new AccountAlreadyOpened(accountId));
    }
}
