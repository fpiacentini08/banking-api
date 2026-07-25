package com.example.banking.domain.user;

import com.example.banking.eventsourcing.fixture.AggregateTestFixture;
import org.junit.jupiter.api.Test;

class UserBehaviourTest {

    private final AggregateTestFixture<User, UserCommand, UserEvent> fixture =
            AggregateTestFixture.forBehaviour(new UserBehaviour());

    @Test
    void rightArm_registerEmitsUserRegistered() {
        UserId id = new UserId("u-1");
        fixture.given()
                .when(new RegisterUser(id, "Ada Lovelace", "ada@example.com"))
                .expectEvents(new UserRegistered(id, "Ada Lovelace", "ada@example.com"));
    }

    @Test
    void leftArm_reRegisterIsRejected() {
        UserId id = new UserId("u-1");
        fixture.given(new UserRegistered(id, "Ada Lovelace", "ada@example.com"))
                .when(new RegisterUser(id, "Ada Lovelace", "ada@example.com"))
                .expectError(new UserAlreadyRegistered(id));
    }
}
