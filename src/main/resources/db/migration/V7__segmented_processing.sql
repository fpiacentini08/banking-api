-- SPIKE: SKIP LOCKED scale-out for the read side (tracking processors).
-- Splits the event stream into fixed segments so multiple pods process disjoint segments
-- concurrently while preserving per-aggregate order (an aggregate always maps to one segment).
-- The segment is derived purely in SQL, so no change to the write path is needed.
-- SEGMENTS (32) MUST match SegmentedEventProcessor.SEGMENTS.
ALTER TABLE domain_event
    ADD COLUMN segment SMALLINT AS (CRC32(aggregate_id) MOD 32) STORED;

CREATE INDEX idx_domain_event_segment ON domain_event (segment, global_position);

-- One resumable token per (processor, segment); the row lock is the SKIP LOCKED claim unit.
CREATE TABLE segment_token (
    processor_name VARCHAR(64) NOT NULL,
    segment        SMALLINT    NOT NULL,
    position       BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (processor_name, segment)
);
