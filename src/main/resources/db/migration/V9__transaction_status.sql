-- Async command-status resource behind the 202 write path: one row per transaction, keyed by the
-- caller-facing transaction_id. Monotonic transitions (PENDING -> COMPLETED | REJECTED | FAILED);
-- terminal states never regress. Generalized beyond account money-transactions: account_id is
-- nullable and result_user_id carries a created id (user registration uses type = 'user-registration').
CREATE TABLE transaction_status (
    transaction_id CHAR(36)     NOT NULL PRIMARY KEY,
    type           VARCHAR(32)  NOT NULL,
    status         VARCHAR(16)  NOT NULL,
    reason         VARCHAR(512) NULL,
    account_id     CHAR(36)     NULL,
    result_user_id CHAR(36)     NULL,
    created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);
