CREATE TABLE saga_instance (
    saga_id    CHAR(36)    NOT NULL PRIMARY KEY,
    saga_type  VARCHAR(64) NOT NULL,
    state      JSON        NOT NULL,
    terminal   BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE saga_association (
    saga_type       VARCHAR(64)  NOT NULL,
    association_key VARCHAR(128) NOT NULL,
    saga_id         CHAR(36)     NOT NULL,
    PRIMARY KEY (saga_type, association_key)
);
