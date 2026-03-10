-- Rollback bitemporality: recreate original tables without temporal columns.
-- WARNING: This discards all historical data, keeping only current versions.

-- 1. Payloads: remove from_block, to_block; restore original unique index
CREATE TABLE payloads_old (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_key BLOB NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    string_attributes TEXT NOT NULL DEFAULT '{}',
    numeric_attributes TEXT NOT NULL DEFAULT '{}'
);

INSERT INTO payloads_old (id, entity_key, payload, content_type, string_attributes, numeric_attributes)
SELECT id, entity_key, payload, content_type, string_attributes, numeric_attributes
FROM payloads
WHERE to_block IS NULL;

DROP TABLE payloads;
ALTER TABLE payloads_old RENAME TO payloads;

CREATE UNIQUE INDEX payloads_entity_key_index ON payloads (entity_key);

-- 2. String attribute bitmaps: keep only the latest keyframe per (name, value)
CREATE TABLE string_attributes_values_bitmaps_old (
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    bitmap BLOB,
    PRIMARY KEY (name, value)
);

INSERT INTO string_attributes_values_bitmaps_old (name, value, bitmap)
SELECT s.name, s.value, s.bitmap
FROM string_attributes_values_bitmaps s
INNER JOIN (
    SELECT name, value, MAX(block) AS max_block
    FROM string_attributes_values_bitmaps
    WHERE is_full_bitmap = 1
    GROUP BY name, value
) latest ON s.name = latest.name AND s.value = latest.value AND s.block = latest.max_block;

DROP TABLE string_attributes_values_bitmaps;
ALTER TABLE string_attributes_values_bitmaps_old RENAME TO string_attributes_values_bitmaps;

-- 3. Numeric attribute bitmaps: keep only the latest keyframe per (name, value)
CREATE TABLE numeric_attributes_values_bitmaps_old (
    name TEXT NOT NULL,
    value INTEGER NOT NULL,
    bitmap BLOB,
    PRIMARY KEY (name, value)
);

INSERT INTO numeric_attributes_values_bitmaps_old (name, value, bitmap)
SELECT n.name, n.value, n.bitmap
FROM numeric_attributes_values_bitmaps n
INNER JOIN (
    SELECT name, value, MAX(block) AS max_block
    FROM numeric_attributes_values_bitmaps
    WHERE is_full_bitmap = 1
    GROUP BY name, value
) latest ON n.name = latest.name AND n.value = latest.value AND n.block = latest.max_block;

DROP TABLE numeric_attributes_values_bitmaps;
ALTER TABLE numeric_attributes_values_bitmaps_old RENAME TO numeric_attributes_values_bitmaps;
