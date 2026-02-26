package store

import (
	"context"
	"strings"
)

const (
	bulkDeleteBitmapChunkSize = 500
	bulkUpsertBitmapChunkSize = 500
)

func (q *Queries) BulkDeleteStringAttributeValueBitmaps(ctx context.Context, args []DeleteStringAttributeValueBitmapParams) error {
	if len(args) == 0 {
		return nil
	}

	for start := 0; start < len(args); start += bulkDeleteBitmapChunkSize {
		end := start + bulkDeleteBitmapChunkSize
		if end > len(args) {
			end = len(args)
		}
		chunk := args[start:end]

		var sb strings.Builder
		sb.Grow(64 + len(chunk)*8)

		sb.WriteString("DELETE FROM ")
		sb.WriteString("string_attributes_values_bitmaps")
		sb.WriteString(" WHERE (name, value) IN (")

		params := make([]any, 0, len(chunk)*2)
		for i, a := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?)")
			params = append(params, a.Name, a.Value)
		}
		sb.WriteString(")")

		_, err := q.db.ExecContext(ctx, sb.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queries) BulkDeleteNumericAttributeValueBitmaps(ctx context.Context, args []DeleteNumericAttributeValueBitmapParams) error {
	if len(args) == 0 {
		return nil
	}

	for start := 0; start < len(args); start += bulkDeleteBitmapChunkSize {
		end := start + bulkDeleteBitmapChunkSize
		if end > len(args) {
			end = len(args)
		}
		chunk := args[start:end]

		var sb strings.Builder
		sb.Grow(64 + len(chunk)*8)

		sb.WriteString("DELETE FROM ")
		sb.WriteString("numeric_attributes_values_bitmaps")
		sb.WriteString(" WHERE (name, value) IN (")

		params := make([]any, 0, len(chunk)*2)
		for i, a := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?)")
			params = append(params, a.Name, a.Value)
		}
		sb.WriteString(")")

		_, err := q.db.ExecContext(ctx, sb.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queries) BulkUpsertStringAttributeValueBitmaps(ctx context.Context, args []UpsertStringAttributeValueBitmapParams) error {
	if len(args) == 0 {
		return nil
	}

	for start := 0; start < len(args); start += bulkUpsertBitmapChunkSize {
		end := start + bulkUpsertBitmapChunkSize
		if end > len(args) {
			end = len(args)
		}
		chunk := args[start:end]

		var sb strings.Builder
		sb.Grow(96 + len(chunk)*12)

		sb.WriteString("INSERT INTO string_attributes_values_bitmaps (name, value, bitmap) VALUES ")

		params := make([]any, 0, len(chunk)*3)
		for i, a := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?)")
			params = append(params, a.Name, a.Value, a.Bitmap)
		}

		sb.WriteString(" ON DUPLICATE KEY UPDATE bitmap = VALUES(bitmap)")

		_, err := q.db.ExecContext(ctx, sb.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Queries) BulkUpsertNumericAttributeValueBitmaps(ctx context.Context, args []UpsertNumericAttributeValueBitmapParams) error {
	if len(args) == 0 {
		return nil
	}

	for start := 0; start < len(args); start += bulkUpsertBitmapChunkSize {
		end := start + bulkUpsertBitmapChunkSize
		if end > len(args) {
			end = len(args)
		}
		chunk := args[start:end]

		var sb strings.Builder
		sb.Grow(96 + len(chunk)*12)

		sb.WriteString("INSERT INTO numeric_attributes_values_bitmaps (name, value, bitmap) VALUES ")

		params := make([]any, 0, len(chunk)*3)
		for i, a := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?)")
			params = append(params, a.Name, a.Value, a.Bitmap)
		}

		sb.WriteString(" ON DUPLICATE KEY UPDATE bitmap = VALUES(bitmap)")

		_, err := q.db.ExecContext(ctx, sb.String(), params...)
		if err != nil {
			return err
		}
	}

	return nil
}

