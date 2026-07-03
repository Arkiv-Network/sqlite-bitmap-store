-- Numeric attribute values are now stored with their top bit flipped
-- (value - 2^63) so that the full uint64 range fits in SQLite's signed
-- INTEGER while preserving order; see store/numeric_value.go.
--
-- Existing rows hold the raw value, which is always < 2^63 (larger values
-- could not be written before this encoding existed), so re-encoding is a
-- plain subtraction. The literal is split in two because SQLite parses
-- 9223372036854775808 as a REAL, which would corrupt the values.
UPDATE numeric_attributes_values_bitmaps
SET value = value - 9223372036854775807 - 1;
