-- name: InsertPayload :exec
INSERT INTO payloads (
    id,
    payload,
    content_type,
    string_attributes,
    numeric_attributes
) VALUES (?, ?, ?, ?, ?);

