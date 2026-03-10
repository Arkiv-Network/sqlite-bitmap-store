-- Bitemporality migration: add temporal columns to payloads and bitmap tables.
-- SQLite does not support ALTER TABLE to change primary keys, so we recreate tables.

-- 1. Payloads: add from_block and to_block columns
CREATE TABLE payloads_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_key BLOB NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    string_attributes TEXT NOT NULL DEFAULT '{}',
    numeric_attributes TEXT NOT NULL DEFAULT '{}',
    from_block INTEGER NOT NULL DEFAULT 0,
    to_block INTEGER -- NULL means "current" / still active
);

INSERT INTO payloads_new (id, entity_key, payload, content_type, string_attributes, numeric_attributes, from_block, to_block)
SELECT id, entity_key, payload, content_type, string_attributes, numeric_attributes, 0, NULL
FROM payloads;

DROP TABLE payloads;
ALTER TABLE payloads_new RENAME TO payloads;

-- Partial unique index: at most one current (to_block IS NULL) row per entity_key
CREATE UNIQUE INDEX payloads_entity_key_current ON payloads (entity_key) WHERE to_block IS NULL;
-- Index for historical lookups
CREATE INDEX payloads_entity_key_blocks ON payloads (entity_key, from_block, to_block);

-- 2. String attribute bitmaps: add block and is_full_bitmap columns, change PK
CREATE TABLE string_attributes_values_bitmaps_new (
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    block INTEGER NOT NULL DEFAULT 0,
    is_full_bitmap INTEGER NOT NULL DEFAULT 1,
    bitmap BLOB,
    PRIMARY KEY (name, value, block)
);

INSERT INTO string_attributes_values_bitmaps_new (name, value, block, is_full_bitmap, bitmap)
SELECT name, value, 0, 1, bitmap
FROM string_attributes_values_bitmaps;

DROP TABLE string_attributes_values_bitmaps;
ALTER TABLE string_attributes_values_bitmaps_new RENAME TO string_attributes_values_bitmaps;

-- 3. Numeric attribute bitmaps: add block and is_full_bitmap columns, change PK
CREATE TABLE numeric_attributes_values_bitmaps_new (
    name TEXT NOT NULL,
    value INTEGER NOT NULL,
    block INTEGER NOT NULL DEFAULT 0,
    is_full_bitmap INTEGER NOT NULL DEFAULT 1,
    bitmap BLOB,
    PRIMARY KEY (name, value, block)
);

INSERT INTO numeric_attributes_values_bitmaps_new (name, value, block, is_full_bitmap, bitmap)
SELECT name, value, 0, 1, bitmap
FROM numeric_attributes_values_bitmaps;

DROP TABLE numeric_attributes_values_bitmaps;
ALTER TABLE numeric_attributes_values_bitmaps_new RENAME TO numeric_attributes_values_bitmaps;
