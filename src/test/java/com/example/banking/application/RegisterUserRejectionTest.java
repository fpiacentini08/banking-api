package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;
import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.UserId;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class RegisterUserRejectionTest {

    @Autowired RegisterUserGateway gateway;
    @Autowired TransactionStatusStore statusStore;

    @Test
    void reRegistrationOfSameUserSettlesRejected() {
        UserId userId = new UserId(UUID.randomUUID().toString());

        TransactionId firstTransaction = new TransactionId(UUID.randomUUID().toString());
        statusStore.insertPending(firstTransaction, "user-registration");
        gateway.submit(firstTransaction, new RegisterUser(userId, "Ada Lovelace", "ada@example.com"));
        await(() -> statusStore.find(firstTransaction)
                .map(status -> status.state() == TransactionState.COMPLETED).orElse(false));

        TransactionId secondTransaction = new TransactionId(UUID.randomUUID().toString());
        statusStore.insertPending(secondTransaction, "user-registration");
        gateway.submit(secondTransaction, new RegisterUser(userId, "Ada Again", "ada2@example.com"));
        await(() -> statusStore.find(secondTransaction)
                .map(status -> status.state() == TransactionState.REJECTED).orElse(false));

        TransactionStatus rejected = statusStore.find(secondTransaction).orElseThrow();
        assertThat(rejected.reason()).contains(userId.value());
    }

    private static void await(BooleanSupplier condition) {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting condition", interrupted);
            }
        }
        throw new AssertionError("condition not met within timeout");
    }
}
