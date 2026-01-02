package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	"github.com/RoaringBitmap/roaring/roaring64"
)

type numericBitmapOps struct {
	st store.Querier
}

func newNumericBitmapOps(st store.Querier) *numericBitmapOps {
	return &numericBitmapOps{st: st}
}

func (o *numericBitmapOps) Add(ctx context.Context, name string, value uint64, id uint64) error {
	bitmap, err := o.st.GetNumericAttributeValueBitmap(
		ctx,
		store.GetNumericAttributeValueBitmapParams{
			Name:  name,
			Value: value,
		},
	)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get numeric attribute %q value %q bitmap: %w", name, value, err)
	}

	bm := roaring64.New()

	if len(bitmap) > 0 {
		err = bm.UnmarshalBinary(bitmap)
		if err != nil {
			return fmt.Errorf("failed to unmarshal numeric attribute %q value %d bitmap: %w", name, value, err)
		}
	}

	bm.Add(id)

	bitmap, err = bm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal numeric attribute %q value %d bitmap: %w", name, value, err)
	}

	err = o.st.UpsertNumericAttributeValueBitmap(
		ctx,
		store.UpsertNumericAttributeValueBitmapParams{
			Name:   name,
			Value:  value,
			Bitmap: bitmap,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to upsert numeric attribute %q value %d bitmap: %w", name, value, err)
	}

	return nil
}

func (o *numericBitmapOps) Remove(ctx context.Context, name string, value uint64, id uint64) error {
	bitmap, err := o.st.GetNumericAttributeValueBitmap(
		ctx,
		store.GetNumericAttributeValueBitmapParams{
			Name:  name,
			Value: value,
		},
	)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get numeric attribute %q value %d bitmap: %w", name, value, err)
	}

	bm := roaring64.New()

	if len(bitmap) > 0 {
		err = bm.UnmarshalBinary(bitmap)
		if err != nil {
			return fmt.Errorf("failed to unmarshal numeric attribute %q value %d bitmap: %w", name, value, err)
		}
	}

	bm.Remove(id)

	if bm.IsEmpty() {
		err = o.st.DeleteNumericAttributeValueBitmap(
			ctx,
			store.DeleteNumericAttributeValueBitmapParams{
				Name:  name,
				Value: value,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to delete numeric attribute %q value %d bitmap: %w", name, value, err)
		}
	} else {
		bitmap, err = bm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal numeric attribute %q value %d bitmap: %w", name, value, err)
		}
		err = o.st.UpsertNumericAttributeValueBitmap(
			ctx,
			store.UpsertNumericAttributeValueBitmapParams{
				Name:   name,
				Value:  value,
				Bitmap: bitmap,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to upsert numeric attribute %q value %d bitmap: %w", name, value, err)
		}
	}
	return nil
}
