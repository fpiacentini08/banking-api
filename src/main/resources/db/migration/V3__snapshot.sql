-- Latest-only aggregate snapshots; replay from zero remains the fallback truth.
CREATE TABLE snapshot (
    aggregate_id CHAR(36) NOT NULL PRIMARY KEY,
    sequence_nr  BIGINT   NOT NULL,
    revision     INT      NOT NULL,
    payload      JSON     NOT NULL
);
