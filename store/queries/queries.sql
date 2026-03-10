-- name: ClosePayloadVersion :exec
UPDATE payloads SET to_block = sqlc.arg(block)
WHERE entity_key = sqlc.arg(entity_key) AND to_block IS NULL;

-- name: InsertPayload :one
INSERT INTO payloads (entity_key, payload, content_type, string_attributes, numeric_attributes, from_block, to_block)
VALUES (?, ?, ?, ?, ?, ?, NULL)
RETURNING id;

-- name: GetCurrentPayloadForEntityKey :one
SELECT entity_key, id, payload, content_type, string_attributes, numeric_attributes, from_block
FROM payloads
WHERE entity_key = sqlc.arg(entity_key) AND to_block IS NULL;

-- name: InsertStringBitmapEntry :exec
INSERT INTO string_attributes_values_bitmaps (name, value, block, is_full_bitmap, bitmap)
VALUES (?, ?, ?, ?, ?);

-- name: InsertNumericBitmapEntry :exec
INSERT INTO numeric_attributes_values_bitmaps (name, value, block, is_full_bitmap, bitmap)
VALUES (?, ?, ?, ?, ?);

-- name: GetLatestStringKeyframeBlock :one
SELECT COALESCE(MAX(block), -1) FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value) AND is_full_bitmap = 1;

-- name: GetLatestNumericKeyframeBlock :one
SELECT COALESCE(MAX(block), -1) FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value) AND is_full_bitmap = 1;

-- name: GetStringKeyframeBlockAtOrBefore :one
SELECT COALESCE(MAX(block), -1) FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value) AND is_full_bitmap = 1
  AND block <= sqlc.arg(target_block);

-- name: GetNumericKeyframeBlockAtOrBefore :one
SELECT COALESCE(MAX(block), -1) FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value) AND is_full_bitmap = 1
  AND block <= sqlc.arg(target_block);

-- name: GetStringBitmapEntriesInRange :many
SELECT block, is_full_bitmap, bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block >= sqlc.arg(from_block) AND block <= sqlc.arg(to_block)
ORDER BY block ASC;

-- name: GetNumericBitmapEntriesInRange :many
SELECT block, is_full_bitmap, bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block >= sqlc.arg(from_block) AND block <= sqlc.arg(to_block)
ORDER BY block ASC;

-- name: GetAllStringBitmapEntriesFrom :many
SELECT block, is_full_bitmap, bitmap FROM string_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block >= sqlc.arg(from_block)
ORDER BY block ASC;

-- name: GetAllNumericBitmapEntriesFrom :many
SELECT block, is_full_bitmap, bitmap FROM numeric_attributes_values_bitmaps
WHERE name = sqlc.arg(name) AND value = sqlc.arg(value)
  AND block >= sqlc.arg(from_block)
ORDER BY block ASC;

-- name: CountDeltasSinceLastKeyframeString :one
SELECT COUNT(*) FROM string_attributes_values_bitmaps AS outer_s
WHERE outer_s.name = sqlc.arg(name) AND outer_s.value = sqlc.arg(value) AND outer_s.is_full_bitmap = 0
  AND outer_s.block > (
    SELECT COALESCE(MAX(inner_s.block), -1)
    FROM string_attributes_values_bitmaps AS inner_s
    WHERE inner_s.name = sqlc.arg(name) AND inner_s.value = sqlc.arg(value) AND inner_s.is_full_bitmap = 1
  );

-- name: CountDeltasSinceLastKeyframeNumeric :one
SELECT COUNT(*) FROM numeric_attributes_values_bitmaps AS outer_n
WHERE outer_n.name = sqlc.arg(name) AND outer_n.value = sqlc.arg(value) AND outer_n.is_full_bitmap = 0
  AND outer_n.block > (
    SELECT COALESCE(MAX(inner_n.block), -1)
    FROM numeric_attributes_values_bitmaps AS inner_n
    WHERE inner_n.name = sqlc.arg(name) AND inner_n.value = sqlc.arg(value) AND inner_n.is_full_bitmap = 1
  );

-- name: UpsertLastBlock :exec
INSERT INTO last_block (id, block)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET block = EXCLUDED.block;

-- name: GetLastBlock :one
SELECT block FROM last_block;

-- Pruning queries

-- name: PrunePayloadsBefore :exec
DELETE FROM payloads WHERE to_block IS NOT NULL AND to_block <= sqlc.arg(block);

-- name: PruneStringBitmapsBefore :exec
DELETE FROM string_attributes_values_bitmaps
WHERE rowid IN (
  SELECT b.rowid FROM string_attributes_values_bitmaps AS b
  WHERE b.block < (
    SELECT COALESCE(MAX(k.block), 0)
    FROM string_attributes_values_bitmaps AS k
    WHERE k.name = b.name AND k.value = b.value
      AND k.is_full_bitmap = 1 AND k.block <= sqlc.arg(prune_block)
  )
);

-- name: PruneNumericBitmapsBefore :exec
DELETE FROM numeric_attributes_values_bitmaps
WHERE rowid IN (
  SELECT b.rowid FROM numeric_attributes_values_bitmaps AS b
  WHERE b.block < (
    SELECT COALESCE(MAX(k.block), 0)
    FROM numeric_attributes_values_bitmaps AS k
    WHERE k.name = b.name AND k.value = b.value
      AND k.is_full_bitmap = 1 AND k.block <= sqlc.arg(prune_block)
  )
);

-- Reorg queries

-- name: ReorgReopenPayloads :exec
UPDATE payloads SET to_block = NULL
WHERE from_block <= sqlc.arg(block) AND to_block IS NOT NULL AND to_block > sqlc.arg(block);

-- name: ReorgDeleteNewPayloads :exec
DELETE FROM payloads WHERE from_block > sqlc.arg(block);

-- name: ReorgDeleteStringBitmaps :exec
DELETE FROM string_attributes_values_bitmaps WHERE block > sqlc.arg(block);

-- name: ReorgDeleteNumericBitmaps :exec
DELETE FROM numeric_attributes_values_bitmaps WHERE block > sqlc.arg(block);
