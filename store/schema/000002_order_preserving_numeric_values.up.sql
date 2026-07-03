-- Re-encode numeric attribute values so that SQL's signed ordering of the
-- stored column matches unsigned numeric ordering across the full uint64
-- range: stored = value XOR 2^63, reinterpreted as signed 64-bit.
--
-- In signed arithmetic the transform is
--     new = old - 2^63   for old >= 0
--     new = old + 2^63   for old <  0
-- which is exact for every int64 (no overflow) and is its own inverse.
--
-- The shift is written as two steps of 9223372036854775807 (int64 max)
-- and 1: the one-step literal 9223372036854775808 does not fit in a
-- 64-bit integer, so SQLite would read it as a floating-point number and
-- silently store imprecise REAL values. Every intermediate below stays
-- inside the int64 range, keeping the arithmetic exact and integer-typed.
--
-- The table is rebuilt rather than updated in place: the transform swaps
-- the two halves of the number line, so an in-place UPDATE could collide
-- with a not-yet-updated row under the (name, value) primary key.
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
