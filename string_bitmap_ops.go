package sqlitebitmapstore

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/arkiv-network/sqlite-bitmap-store/store"
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
	if err != nil {
		return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
	}

	bm := roaring64.New()

	err = bm.UnmarshalBinary(bitmap)
	if err != nil {
		return fmt.Errorf("failed to unmarshal string attribute %q value %q bitmap: %w", name, value, err)
	}

	bm.Add(id)

	bitmap, err = bm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal string attribute %q value %q bitmap: %w", name, value, err)
	}

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
	if err != nil {
		return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
	}

	bm := roaring64.New()

	err = bm.UnmarshalBinary(bitmap)
	if err != nil {
		return fmt.Errorf("failed to unmarshal string attribute %q value %q bitmap: %w", name, value, err)
	}

	bm.Remove(id)

	if bm.IsEmpty() {
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
		bitmap, err = bm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal string attribute %q value %q bitmap: %w", name, value, err)
		}
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
