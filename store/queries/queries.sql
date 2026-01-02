-- name: InsertPayload :exec
INSERT INTO payloads (
    id,
    payload,
    content_type,
    string_attributes,
    numeric_attributes
) VALUES (?, ?, ?, ?, ?);

-- name: UpsertAttributeValueBitmap :exec
INSERT INTO ATTRIBUTES_VALUES_BITMAPS (name, value, type, bitmap)
VALUES (?, ?, ?, ?)
ON CONFLICT (name, value, type) DO UPDATE SET bitmap = excluded.bitmap;

