CREATE TABLE last_block (
    id INTEGER NOT NULL DEFAULT 1 CHECK (id = 1),
    block BIGINT NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;