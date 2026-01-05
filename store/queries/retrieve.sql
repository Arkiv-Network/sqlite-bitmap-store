-- name: RetrievePayloads :many
SELECT entity_key, id, payload, content_type, string_attributes, numeric_attributes
FROM payloads
WHERE id IN (sqlc.slice(ids))
ORDER BY id DESC;
