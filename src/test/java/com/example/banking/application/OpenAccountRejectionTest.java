package com.example.banking.application;

import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.domain.user.UserId;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class OpenAccountRejectionTest {

    @Autowired OpenAccountGateway gateway;
    @Autowired TransactionStatusStore statusStore;

    @Test
    void reopeningTheSameAccountSettlesRejected() {
        AccountId accountId = new AccountId(UUID.randomUUID().toString());
        UserId ownerId = new UserId(UUID.randomUUID().toString());

        String firstTransaction = UUID.randomUUID().toString();
        statusStore.insertPending(firstTransaction, "account-opening");
        gateway.submit(firstTransaction, new OpenAccount(accountId, ownerId));
        await(() -> statusStore.find(firstTransaction)
                .map(status -> status.state() == TransactionState.COMPLETED).orElse(false));

        String secondTransaction = UUID.randomUUID().toString();
        statusStore.insertPending(secondTransaction, "account-opening");
        gateway.submit(secondTransaction, new OpenAccount(accountId, ownerId));   // same accountId
        await(() -> statusStore.find(secondTransaction)
                .map(status -> status.state() == TransactionState.REJECTED).orElse(false));

        TransactionStatus rejected = statusStore.find(secondTransaction).orElseThrow();
        assertThat(rejected.reason()).contains(accountId.value());   // AccountAlreadyOpened.message()
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
