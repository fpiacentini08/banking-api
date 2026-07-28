package com.example.banking.adapter.out.projection;

import com.example.banking.application.TransactionState;
import com.example.banking.application.TransactionStatus;
import com.example.banking.application.TransactionStatusStore;
import com.example.banking.domain.shared.TransactionId;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

@FullContextTest
class JooqTransactionStatusStoreTest {

    @Autowired TransactionStatusStore store;

    @Test
    void terminalStateDoesNotRegressAndDuplicateWriteIsNoOp() {
        TransactionId txId = new TransactionId("tx-status-terminal");
        String userId = "user-status-terminal";
        store.insertPending(txId, "user-registration");
        store.markCompleted(txId, userId);

        store.markRejected(txId, "late rejection");   // not PENDING -> guarded no-op
        store.markCompleted(txId, "other-user");       // duplicate terminal -> no-op

        TransactionStatus status = store.find(txId).orElseThrow();
        assertThat(status.state()).isEqualTo(TransactionState.COMPLETED);
        assertThat(status.resultUserId()).isEqualTo(userId);
        assertThat(status.reason()).isNull();
    }

    @Test
    void recordsRejectedWithReason() {
        TransactionId txId = new TransactionId("tx-status-rejected");
        store.insertPending(txId, "user-registration");

        store.markRejected(txId, "nope");

        TransactionStatus status = store.find(txId).orElseThrow();
        assertThat(status.state()).isEqualTo(TransactionState.REJECTED);
        assertThat(status.reason()).isEqualTo("nope");
    }

    @Test
    void recordsFailedWithReason() {
        TransactionId txId = new TransactionId("tx-status-failed");
        store.insertPending(txId, "user-registration");

        store.markFailed(txId, "boom");

        TransactionStatus status = store.find(txId).orElseThrow();
        assertThat(status.state()).isEqualTo(TransactionState.FAILED);
        assertThat(status.reason()).isEqualTo("boom");
    }
}
