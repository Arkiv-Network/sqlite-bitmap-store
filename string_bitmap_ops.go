package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

type stringBitmapOps struct {
	st store.Querier
}

func newStringBitmapOps(st store.Querier) *stringBitmapOps {
	return &stringBitmapOps{st: st}
}

func (o *stringBitmapOps) Add(ctx context.Context, name string, value string, id uint64) error {
	bitmap, err := o.st.GetStringAttributeValueBitmap(
		ctx,
		store.GetStringAttributeValueBitmapParams{
			Name:  name,
			Value: value,
		},
	)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
	}

	if bitmap == nil {
		bitmap = store.NewBitmap()
	}

	bitmap.Add(id)

	err = o.st.UpsertStringAttributeValueBitmap(
		ctx,
		store.UpsertStringAttributeValueBitmapParams{
			Name:   name,
			Value:  value,
			Bitmap: bitmap,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to upsert string attribute %q value %q bitmap: %w", name, value, err)
	}

	return nil
}

func (o *stringBitmapOps) Remove(ctx context.Context, name string, value string, id uint64) error {
	bitmap, err := o.st.GetStringAttributeValueBitmap(
		ctx,
		store.GetStringAttributeValueBitmapParams{
			Name:  name,
			Value: value,
		},
	)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
	}

	if bitmap == nil {
		bitmap = store.NewBitmap()
	}

	bitmap.Remove(id)

	if bitmap.IsEmpty() {
		err = o.st.DeleteStringAttributeValueBitmap(
			ctx,
			store.DeleteStringAttributeValueBitmapParams{
				Name:  name,
				Value: value,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to delete string attribute %q value %q bitmap: %w", name, value, err)
		}
	} else {
		err = o.st.UpsertStringAttributeValueBitmap(
			ctx,
			store.UpsertStringAttributeValueBitmapParams{
				Name:   name,
				Value:  value,
				Bitmap: bitmap,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to upsert string attribute %q value %q bitmap: %w", name, value, err)
		}
	}
	return nil
}
