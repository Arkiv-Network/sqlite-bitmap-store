
CREATE TABLE payloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_key BLOB NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    string_attributes TEXT NOT NULL DEFAULT '{}',
    numeric_attributes TEXT NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX payloads_entity_key_index ON payloads (entity_key);

CREATE TABLE last_block (
    id INTEGER NOT NULL DEFAULT 1 CHECK (id = 1),
    block INTEGER NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO last_block (id, block) VALUES (1, 0);

CREATE TABLE STRING_ATTRIBUTES_VALUES_BITMAPS (
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    bitmap BLOB,
    PRIMARY KEY (name, value)
);


CREATE TABLE NUMERIC_ATTRIBUTES_VALUES_BITMAPS (
    name TEXT NOT NULL,
    value INTEGER NOT NULL,
    bitmap BLOB,
    PRIMARY KEY (name, value)
);


