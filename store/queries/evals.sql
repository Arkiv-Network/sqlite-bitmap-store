-- name: EvaluateStringAttributeValueEqual :one
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value);

-- name: EvaluateNumericAttributeValueEqual :one
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value);


-- name: EvaluateStringAttributeValueNotEqual :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value != sqlc.arg(value);

-- name: EvaluateNumericAttributeValueNotEqual :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value != sqlc.arg(value);

-- name: EvaluateStringAttributeValueLowerThan :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value < sqlc.arg(value);

-- name: EvaluateStringAttributeValueGreaterThan :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value > sqlc.arg(value);

-- name: EvaluateStringAttributeValueLessOrEqualThan :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value <= sqlc.arg(value);

-- name: EvaluateStringAttributeValueGreaterOrEqualThan :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value >= sqlc.arg(value);

-- name: EvaluateStringAttributeValueGlob :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value GLOB sqlc.arg(value);

-- name: EvaluateStringAttributeValueNotGlob :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT GLOB sqlc.arg(value);

-- name: EvaluateStringAttributeValueNotInclusion :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT IN (sqlc.Slice('values'));

-- name: EvaluateStringAttributeValueInclusion :many
SELECT bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value IN (sqlc.Slice('values'));

-- name: EvaluateNumericAttributeValueLowerThan :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value < sqlc.arg(value);

-- name: EvaluateNumericAttributeValueGreaterThan :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value > sqlc.arg(value);

-- name: EvaluateNumericAttributeValueLessOrEqualThan :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value <= sqlc.arg(value);

-- name: EvaluateNumericAttributeValueGreaterOrEqualThan :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value >= sqlc.arg(value);

-- name: EvaluateNumericAttributeValueInclusion :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value IN (sqlc.Slice('values'));

-- name: EvaluateNumericAttributeValueNotInclusion :many
SELECT bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value NOT IN (sqlc.Slice('values'));
