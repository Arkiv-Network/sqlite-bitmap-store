CREATE TABLE payloads (
    id UNSIGNED BIG INT NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    string_attributes TEXT NOT NULL DEFAULT '{}',
    numeric_attributes TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (entity_key, from_block)
);

CREATE TABLE last_block (
    id INTEGER NOT NULL DEFAULT 1 CHECK (id = 1),
    block INTEGER NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO last_block (id, block) VALUES (1, 0);

CREATE TABLE ATTRIBUTES_VALUES_BITMAPS (
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('string', 'numeric')),
    bitmap BLOB,
    PRIMARY KEY (name, value, type)
)

