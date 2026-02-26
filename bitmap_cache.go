package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	"golang.org/x/sync/errgroup"
)

type nameValue[T any] struct {
	name  string
	value T
}

type bitmapCache struct {
	st *store.Queries

	stringBitmaps  map[nameValue[string]]*store.Bitmap
	numericBitmaps map[nameValue[uint64]]*store.Bitmap
}

func newBitmapCache(st *store.Queries) *bitmapCache {
	return &bitmapCache{
		st:             st,
		stringBitmaps:  make(map[nameValue[string]]*store.Bitmap),
		numericBitmaps: make(map[nameValue[uint64]]*store.Bitmap),
	}
}

func (c *bitmapCache) AddToStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetStringAttributeValueBitmap(ctx, store.GetStringAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.stringBitmaps[k] = bitmap
	}

	bitmap.Add(id)

	return nil

}

func (c *bitmapCache) RemoveFromStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetStringAttributeValueBitmap(ctx, store.GetStringAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get string attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.stringBitmaps[k] = bitmap
	}

	bitmap.Remove(id)

	return nil

}

func (c *bitmapCache) AddToNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetNumericAttributeValueBitmap(ctx, store.GetNumericAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get numeric attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.numericBitmaps[k] = bitmap
	}

	bitmap.Add(id)

	return nil

}

func (c *bitmapCache) RemoveFromNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.GetNumericAttributeValueBitmap(ctx, store.GetNumericAttributeValueBitmapParams{Name: name, Value: value})

		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("failed to get numeric attribute %q value %q bitmap: %w", name, value, err)
		}

		if bitmap == nil {
			bitmap = store.NewBitmap()
		}

		c.numericBitmaps[k] = bitmap
	}

	bitmap.Remove(id)

	return nil

}

func (c *bitmapCache) Flush(ctx context.Context) (err error) {

	eg := &errgroup.Group{}

	eg.SetLimit(runtime.NumCPU())

	for _, bitmap := range c.stringBitmaps {
		if bitmap.IsEmpty() {
			continue
		}
		b := bitmap
		eg.Go(func() error {
			b.RunOptimize()
			return nil
		})
	}

	for _, bitmap := range c.numericBitmaps {
		if bitmap.IsEmpty() {
			continue
		}
		b := bitmap
		eg.Go(func() error {
			b.RunOptimize()
			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to run optimize: %w", err)
	}

	stringDeletes := make([]store.DeleteStringAttributeValueBitmapParams, 0)
	stringUpserts := make([]store.UpsertStringAttributeValueBitmapParams, 0)

	for k, bitmap := range c.stringBitmaps {
		if bitmap.IsEmpty() {
			stringDeletes = append(stringDeletes, store.DeleteStringAttributeValueBitmapParams{Name: k.name, Value: k.value})
			continue
		}
		stringUpserts = append(stringUpserts, store.UpsertStringAttributeValueBitmapParams{Name: k.name, Value: k.value, Bitmap: bitmap})
	}

	err = c.st.BulkDeleteStringAttributeValueBitmaps(ctx, stringDeletes)
	if err != nil {
		return fmt.Errorf("failed to bulk delete string attribute value bitmaps: %w", err)
	}

	err = c.st.BulkUpsertStringAttributeValueBitmaps(ctx, stringUpserts)
	if err != nil {
		return fmt.Errorf("failed to bulk upsert string attribute value bitmaps: %w", err)
	}

	numericDeletes := make([]store.DeleteNumericAttributeValueBitmapParams, 0)
	numericUpserts := make([]store.UpsertNumericAttributeValueBitmapParams, 0)

	for k, bitmap := range c.numericBitmaps {
		if bitmap.IsEmpty() {
			numericDeletes = append(numericDeletes, store.DeleteNumericAttributeValueBitmapParams{Name: k.name, Value: k.value})
			continue
		}
		numericUpserts = append(numericUpserts, store.UpsertNumericAttributeValueBitmapParams{Name: k.name, Value: k.value, Bitmap: bitmap})
	}

	err = c.st.BulkDeleteNumericAttributeValueBitmaps(ctx, numericDeletes)
	if err != nil {
		return fmt.Errorf("failed to bulk delete numeric attribute value bitmaps: %w", err)
	}

	err = c.st.BulkUpsertNumericAttributeValueBitmaps(ctx, numericUpserts)
	if err != nil {
		return fmt.Errorf("failed to bulk upsert numeric attribute value bitmaps: %w", err)
	}
	return nil
}
