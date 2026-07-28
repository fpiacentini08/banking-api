package com.example.banking.application;

import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.domain.shared.TransactionId;
import com.example.banking.domain.user.UserId;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class OpenAccountGatewayTest {

    @Autowired OpenAccountGateway gateway;
    @Autowired TransactionStatusStore statusStore;
    @Autowired JdbcTemplate jdbc;

    @Test
    void dispatchedOpenCompletesAndProjectsAccount() {
        TransactionId transactionId = new TransactionId("tx-account-gateway");
        AccountId accountId = new AccountId("account-account-gateway");
        UserId ownerId = new UserId("owner-account-gateway");
        statusStore.insertPending(transactionId, "account-opening");

        gateway.submit(transactionId, new OpenAccount(accountId, ownerId));

        await(() -> statusStore.find(transactionId)
                .map(status -> status.state() == TransactionState.COMPLETED)
                .orElse(false));

        await(() -> !jdbc.queryForList("SELECT 1 FROM account_balance WHERE account_id = ?",
                accountId.value()).isEmpty());
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT owner_id, balance FROM account_balance WHERE account_id = ?", accountId.value());
        assertThat(row).containsEntry("owner_id", ownerId.value());
        assertThat(((Number) row.get("balance")).longValue()).isEqualTo(0L);
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
