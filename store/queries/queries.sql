-- name: UpsertPayload :one
INSERT INTO payloads (
    entity_key,
    payload,
    content_type,
    string_attributes,
    numeric_attributes
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (entity_key) DO UPDATE SET
    payload = excluded.payload,
    content_type = excluded.content_type,
    string_attributes = excluded.string_attributes,
    numeric_attributes = excluded.numeric_attributes
RETURNING id;

-- name: DeletePayloadForEntityKey :exec
DELETE FROM payloads
WHERE entity_key = ?;

-- name: GetPayloadForEntityKey :one
SELECT entity_key, id, payload, content_type, string_attributes, numeric_attributes
FROM payloads
WHERE entity_key = ?;

-- name: UpsertStringAttributeValueBitmap :exec
INSERT INTO STRING_ATTRIBUTES_VALUES_BITMAPS (name, value, bitmap)
VALUES (?, ?, ?)
ON CONFLICT (name, value) DO UPDATE SET bitmap = excluded.bitmap;

-- name: DeleteStringAttributeValueBitmap :exec
DELETE FROM STRING_ATTRIBUTES_VALUES_BITMAPS
WHERE name = ? AND value = ?;

-- name: GetStringAttributeValueBitmap :one
SELECT bitmap FROM STRING_ATTRIBUTES_VALUES_BITMAPS
WHERE name = ? AND value = ?;

-- name: UpsertNumericAttributeValueBitmap :exec
INSERT INTO NUMERIC_ATTRIBUTES_VALUES_BITMAPS (name, value, bitmap)
VALUES (?, ?, ?)
ON CONFLICT (name, value) DO UPDATE SET bitmap = excluded.bitmap;

-- name: DeleteNumericAttributeValueBitmap :exec
DELETE FROM NUMERIC_ATTRIBUTES_VALUES_BITMAPS
WHERE name = ? AND value = ?;

-- name: GetNumericAttributeValueBitmap :one
SELECT bitmap FROM NUMERIC_ATTRIBUTES_VALUES_BITMAPS
WHERE name = ? AND value = ?;

-- name: UpsertLastBlock :exec
INSERT INTO last_block (id, block)
VALUES (1, ?)
ON CONFLICT (id) DO UPDATE SET block = EXCLUDED.block;

-- name: GetLastBlock :one
SELECT block FROM last_block;
