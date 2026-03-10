-- name: RetrievePayloads :many
SELECT entity_key, id, payload, content_type, string_attributes, numeric_attributes
FROM payloads
WHERE id IN (sqlc.slice(ids))
ORDER BY id DESC;

-- name: EvaluateAllCurrent :many
SELECT id FROM payloads
WHERE to_block IS NULL
ORDER BY id DESC;

-- name: EvaluateAllAtBlock :many
SELECT id FROM payloads
WHERE from_block <= sqlc.arg(block) AND (to_block IS NULL OR to_block > sqlc.arg(block))
ORDER BY id DESC;

-- name: GetNumberOfEntities :one
SELECT COUNT(*) FROM payloads WHERE to_block IS NULL;
