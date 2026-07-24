-- Scheduled saga timeouts; delivered by DeadlinePoller and deleted on delivery or cancel.
CREATE TABLE deadline (
    deadline_id  VARCHAR(128) NOT NULL PRIMARY KEY,
    saga_type    VARCHAR(64)  NOT NULL,
    saga_id      CHAR(36)     NOT NULL,
    due_at       TIMESTAMP(6) NOT NULL,
    payload_type VARCHAR(128) NOT NULL,
    payload      JSON         NOT NULL,
    KEY idx_deadline_due (due_at)
);
