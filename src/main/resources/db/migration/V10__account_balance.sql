-- Read model: one row per opened account, built by the account-balance-projection tracking processor
-- from AccountOpened events. balance is integer minor units (cents), EUR fixed; seeded 0 at open and
-- updated by deposits/withdrawals (later milestones). updated_at is the event envelope occurred_at.
CREATE TABLE account_balance (
    account_id CHAR(36)     NOT NULL PRIMARY KEY,
    owner_id   CHAR(36)     NOT NULL,
    balance    BIGINT       NOT NULL DEFAULT 0,
    version    BIGINT       NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL
);
