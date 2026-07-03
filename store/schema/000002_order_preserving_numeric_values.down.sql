-- The encoding transform (XOR with 2^63) is its own inverse, so undoing
-- the migration applies the exact same rebuild as the up migration.
-- See the up migration for why the shift is split into two in-range steps.
CREATE TABLE numeric_attributes_values_bitmaps_new (
    name TEXT NOT NULL,
    value INTEGER NOT NULL,
    bitmap BLOB,
    PRIMARY KEY (name, value)
);

INSERT INTO numeric_attributes_values_bitmaps_new (name, value, bitmap)
SELECT name,
       CASE WHEN value >= 0 THEN (value - 9223372036854775807) - 1
            ELSE (value + 9223372036854775807) + 1
       END,
       bitmap
FROM numeric_attributes_values_bitmaps;

DROP TABLE numeric_attributes_values_bitmaps;

ALTER TABLE numeric_attributes_values_bitmaps_new RENAME TO numeric_attributes_values_bitmaps;
