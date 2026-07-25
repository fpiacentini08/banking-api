package com.example.banking.application;

import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.UserId;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class RegisterUserGatewayTest {

    @Autowired RegisterUserGateway gateway;
    @Autowired TransactionStatusStore statusStore;
    @Autowired JdbcTemplate jdbc;

    @Test
    void dispatchedRegistrationCompletesAndProjectsUser() {
        String transactionId = UUID.randomUUID().toString();
        UserId userId = new UserId(UUID.randomUUID().toString());
        statusStore.insertPending(transactionId, "user-registration");

        gateway.submit(transactionId, new RegisterUser(userId, "Ada Lovelace", "ada@example.com"));

        await(() -> statusStore.find(transactionId)
                .map(status -> status.state() == TransactionState.COMPLETED)
                .orElse(false));
        TransactionStatus status = statusStore.find(transactionId).orElseThrow();
        assertThat(status.resultUserId()).isEqualTo(userId.value());

        await(() -> !jdbc.queryForList("SELECT 1 FROM users WHERE user_id = ?", userId.value()).isEmpty());
        Map<String, Object> row =
                jdbc.queryForMap("SELECT name, email FROM users WHERE user_id = ?", userId.value());
        assertThat(row).containsEntry("name", "Ada Lovelace").containsEntry("email", "ada@example.com");
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
