-- Value-listing queries for query evaluation.
-- Each returns distinct values matching a condition, filtered by block.
-- The Go layer then reconstructs bitmaps for each matching value via XOR chains.

-- String attribute value queries

-- name: GetMatchingStringValuesEqual :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesNotEqual :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value != sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesLessThan :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value < sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesGreaterThan :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value > sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesLessOrEqualThan :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value <= sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesGreaterOrEqualThan :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value >= sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesGlob :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value GLOB sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesNotGlob :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT GLOB sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesInclusion :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value IN (sqlc.slice('values'))
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingStringValuesNotInclusion :many
SELECT DISTINCT value FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT IN (sqlc.slice('values'))
  AND block <= sqlc.arg(target_block);

-- Numeric attribute value queries

-- name: GetMatchingNumericValuesEqual :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesNotEqual :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value != sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesLessThan :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value < sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesGreaterThan :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value > sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesLessOrEqualThan :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value <= sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesGreaterOrEqualThan :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value >= sqlc.arg(value)
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesInclusion :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value IN (sqlc.slice('values'))
  AND block <= sqlc.arg(target_block);

-- name: GetMatchingNumericValuesNotInclusion :many
SELECT DISTINCT value FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT IN (sqlc.slice('values'))
  AND block <= sqlc.arg(target_block);
