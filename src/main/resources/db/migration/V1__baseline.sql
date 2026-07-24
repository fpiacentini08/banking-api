-- Baseline migration. Projection tables are added by later milestones.
CREATE TABLE schema_marker (
    id TINYINT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_marker (id) VALUES (1);
