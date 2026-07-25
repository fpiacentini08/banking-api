-- Read model: one row per registered user, built by the users-projection tracking processor
-- from UserRegistered events. registered_at is the event envelope occurred_at (design spec §6).
CREATE TABLE users (
    user_id       CHAR(36)     NOT NULL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    email         VARCHAR(255) NOT NULL,
    registered_at TIMESTAMP(6) NOT NULL
);
