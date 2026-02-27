package store

import (
	"context"
	"strings"
)

const (
	bulkUpsertPayloadChunkSize = 200
	bulkSelectKeysChunkSize    = 500
)

func (q *Queries) BulkUpsertPayloads(ctx context.Context, args []UpsertPayloadParams) error {
	if len(args) == 0 {
		return nil
	}

	for start := 0; start < len(args); start += bulkUpsertPayloadChunkSize {
		end := start + bulkUpsertPayloadChunkSize
		if end > len(args) {
			end = len(args)
		}
		chunk := args[start:end]

		var sb strings.Builder
		sb.Grow(128 + len(chunk)*24)

		sb.WriteString("INSERT INTO payloads (entity_key, payload, content_type, string_attributes, numeric_attributes) VALUES ")

		params := make([]any, 0, len(chunk)*5)
		for i, a := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?,?,?)")
			params = append(params, a.EntityKey, a.Payload, a.ContentType, a.StringAttributes, a.NumericAttributes)
		}

		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		sb.WriteString("id = LAST_INSERT_ID(id), ")
		sb.WriteString("payload = VALUES(payload), ")
		sb.WriteString("content_type = VALUES(content_type), ")
		sb.WriteString("string_attributes = VALUES(string_attributes), ")
		sb.WriteString("numeric_attributes = VALUES(numeric_attributes)")

		_, err := q.db.ExecContext(ctx, sb.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetPayloadIDsForEntityKeys returns a map keyed by string(entity_key) => id.
func (q *Queries) GetPayloadIDsForEntityKeys(ctx context.Context, entityKeys [][]byte) (map[string]uint64, error) {
	out := make(map[string]uint64, len(entityKeys))
	if len(entityKeys) == 0 {
		return out, nil
	}

	for start := 0; start < len(entityKeys); start += bulkSelectKeysChunkSize {
		end := start + bulkSelectKeysChunkSize
		if end > len(entityKeys) {
			end = len(entityKeys)
		}
		chunk := entityKeys[start:end]

		var sb strings.Builder
		sb.Grow(64 + len(chunk)*2)
		sb.WriteString("SELECT entity_key, id FROM payloads WHERE entity_key IN (")
		params := make([]any, 0, len(chunk))
		for i, k := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("?")
			params = append(params, k)
		}
		sb.WriteString(")")

		rows, err := q.db.QueryContext(ctx, sb.String(), params...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var ek []byte
			var id uint64
			if err := rows.Scan(&ek, &id); err != nil {
				rows.Close()
				return nil, err
			}
			out[string(ek)] = id
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

