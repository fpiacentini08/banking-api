-- One row per tracking processor: the last global_position it has applied.
CREATE TABLE tracking_token (
    processor_name VARCHAR(64) NOT NULL PRIMARY KEY,
    position       BIGINT      NOT NULL,
    updated_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);
