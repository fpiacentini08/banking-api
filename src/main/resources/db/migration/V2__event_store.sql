-- Event store: source of truth. global_position is assigned from event_store_sequence
-- under FOR UPDATE in the same transaction as the insert, so commit order == position
-- order and rollbacks leave no gaps (see design spec §4).
CREATE TABLE event_store_sequence (
    id            TINYINT NOT NULL PRIMARY KEY,
    next_position BIGINT  NOT NULL
);
INSERT INTO event_store_sequence (id, next_position) VALUES (1, 1);

CREATE TABLE domain_event (
    global_position BIGINT       NOT NULL PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    CHAR(36)     NOT NULL,
    sequence_nr     BIGINT       NOT NULL,
    event_id        CHAR(36)     NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    revision        INT          NOT NULL,
    payload         JSON         NOT NULL,
    metadata        JSON         NOT NULL,
    occurred_at     TIMESTAMP(6) NOT NULL,
    UNIQUE KEY uq_stream (aggregate_id, sequence_nr)
);
