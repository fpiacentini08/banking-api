package com.example.banking.adapter.out.projection;

import com.example.banking.application.TransactionState;
import com.example.banking.application.TransactionStatus;
import com.example.banking.application.TransactionStatusStore;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class JdbcTransactionStatusStore implements TransactionStatusStore {

    private final JdbcTemplate jdbc;

    public JdbcTransactionStatusStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insertPending(String transactionId, String type) {
        jdbc.update("INSERT INTO transaction_status (transaction_id, type, status) VALUES (?, ?, 'PENDING')",
                transactionId, type);
    }

    @Override
    public void markCompleted(String transactionId, String resultUserId) {
        jdbc.update("UPDATE transaction_status SET status = 'COMPLETED', result_user_id = ? "
                + "WHERE transaction_id = ? AND status = 'PENDING'", resultUserId, transactionId);
    }

    @Override
    public void markRejected(String transactionId, String reason) {
        jdbc.update("UPDATE transaction_status SET status = 'REJECTED', reason = ? "
                + "WHERE transaction_id = ? AND status = 'PENDING'", reason, transactionId);
    }

    @Override
    public void markFailed(String transactionId, String reason) {
        jdbc.update("UPDATE transaction_status SET status = 'FAILED', reason = ? "
                + "WHERE transaction_id = ? AND status = 'PENDING'", reason, transactionId);
    }

    @Override
    public Optional<TransactionStatus> find(String transactionId) {
        List<TransactionStatus> rows = jdbc.query(
                "SELECT transaction_id, type, status, result_user_id, reason "
                        + "FROM transaction_status WHERE transaction_id = ?",
                (rs, rowNum) -> new TransactionStatus(
                        rs.getString("transaction_id"),
                        rs.getString("type"),
                        TransactionState.valueOf(rs.getString("status")),
                        rs.getString("result_user_id"),
                        rs.getString("reason")),
                transactionId);
        return rows.stream().findFirst();
    }
}
