-- name: UpsertPayload :execresult
INSERT INTO
    payloads (
        entity_key,
        payload,
        content_type,
        string_attributes,
        numeric_attributes
    )
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    id = LAST_INSERT_ID(id),
    payload = VALUES(payload),
    content_type = VALUES(content_type),
    string_attributes = VALUES(string_attributes),
    numeric_attributes = VALUES(numeric_attributes);

-- name: DeletePayloadForEntityKey :exec
DELETE FROM payloads WHERE entity_key = ?;

-- name: GetPayloadForEntityKey :one
SELECT
    entity_key,
    id,
    payload,
    content_type,
    string_attributes,
    numeric_attributes
FROM payloads
WHERE
    entity_key = ?;

-- name: UpsertStringAttributeValueBitmap :exec
INSERT INTO
    string_attributes_values_bitmaps (name, value, bitmap)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
    bitmap = VALUES(bitmap);

-- name: DeleteStringAttributeValueBitmap :exec
DELETE FROM string_attributes_values_bitmaps
WHERE
    name = ?
    AND value = ?;

-- name: GetStringAttributeValueBitmap :one
SELECT bitmap
FROM
    string_attributes_values_bitmaps
WHERE
    name = ?
    AND value = ?;

-- name: UpsertNumericAttributeValueBitmap :exec
INSERT INTO
    numeric_attributes_values_bitmaps (name, value, bitmap)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
    bitmap = VALUES(bitmap);

-- name: DeleteNumericAttributeValueBitmap :exec
DELETE FROM numeric_attributes_values_bitmaps
WHERE
    name = ?
    AND value = ?;

-- name: GetNumericAttributeValueBitmap :one
SELECT bitmap
FROM
    numeric_attributes_values_bitmaps
WHERE
    name = ?
    AND value = ?;

-- name: UpsertLastBlock :exec
INSERT INTO
    last_block (id, block)
VALUES (1, ?)
ON DUPLICATE KEY UPDATE
    block = VALUES(block);

-- name: GetLastBlock :one
SELECT block FROM last_block;

-- name: DoltCommit :exec
CALL DOLT_COMMIT ('-m', sqlc.arg (message));

-- name: DoltAdd :exec
CALL DOLT_ADD ('.');