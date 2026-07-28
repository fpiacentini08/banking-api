package com.example.banking.adapter.out.projection;

import com.example.banking.application.TransactionState;
import com.example.banking.application.TransactionStatus;
import com.example.banking.application.TransactionStatusStore;
import com.example.banking.domain.shared.TransactionId;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class JooqTransactionStatusStore implements TransactionStatusStore {

    private final DSLContext dsl;

    public JooqTransactionStatusStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Override
    public void insertPending(TransactionId transactionId, String type) {
        dsl.insertInto(table("transaction_status"))
                .columns(field("transaction_id"), field("type"), field("status"))
                .values(transactionId.value(), type, "PENDING")
                .execute();
    }

    @Override
    public void markCompleted(TransactionId transactionId, String resultUserId) {
        dsl.update(table("transaction_status"))
                .set(field("status"), "COMPLETED")
                .set(field("result_user_id"), resultUserId)
                .where(pendingWithId(transactionId))
                .execute();
    }

    @Override
    public void markRejected(TransactionId transactionId, String reason) {
        dsl.update(table("transaction_status"))
                .set(field("status"), "REJECTED")
                .set(field("reason"), reason)
                .where(pendingWithId(transactionId))
                .execute();
    }

    @Override
    public void markFailed(TransactionId transactionId, String reason) {
        dsl.update(table("transaction_status"))
                .set(field("status"), "FAILED")
                .set(field("reason"), reason)
                .where(pendingWithId(transactionId))
                .execute();
    }

    @Override
    public Optional<TransactionStatus> find(TransactionId transactionId) {
        return dsl.select(field("transaction_id"), field("type"), field("status"),
                        field("result_user_id"), field("reason"))
                .from(table("transaction_status"))
                .where(field("transaction_id").eq(transactionId.value()))
                .fetchOptional()
                .map(r -> new TransactionStatus(
                        new TransactionId(r.get("transaction_id", String.class)),
                        r.get("type", String.class),
                        TransactionState.valueOf(r.get("status", String.class)),
                        r.get("result_user_id", String.class),
                        r.get("reason", String.class)));
    }

    private static Condition pendingWithId(TransactionId transactionId) {
        return field("transaction_id").eq(transactionId.value()).and(field("status").eq("PENDING"));
    }
}
