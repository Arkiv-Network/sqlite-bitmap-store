package sqlitebitmapstore

import (
	"context"
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
	st    *store.Queries
	block uint64

	stringBitmaps  map[nameValue[string]]*store.Bitmap
	numericBitmaps map[nameValue[uint64]]*store.Bitmap

	// Old bitmaps (state before modifications) for computing deltas.
	stringOldBitmaps  map[nameValue[string]]*store.Bitmap
	numericOldBitmaps map[nameValue[uint64]]*store.Bitmap
}

func newBitmapCache(st *store.Queries, block uint64) *bitmapCache {
	return &bitmapCache{
		st:                st,
		block:             block,
		stringBitmaps:     make(map[nameValue[string]]*store.Bitmap),
		numericBitmaps:    make(map[nameValue[uint64]]*store.Bitmap),
		stringOldBitmaps:  make(map[nameValue[string]]*store.Bitmap),
		numericOldBitmaps: make(map[nameValue[uint64]]*store.Bitmap),
	}
}

func cloneBitmap(bm *store.Bitmap) *store.Bitmap {
	if bm == nil || bm.Bitmap == nil {
		return store.NewBitmap()
	}
	return &store.Bitmap{Bitmap: bm.Bitmap.Clone()}
}

func (c *bitmapCache) AddToStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.ReconstructLatestStringBitmap(ctx, name, value)
		if err != nil {
			return fmt.Errorf("failed to reconstruct string bitmap %q=%q: %w", name, value, err)
		}
		c.stringOldBitmaps[k] = cloneBitmap(bitmap)
		c.stringBitmaps[k] = bitmap
	}

	bitmap.Add(id)
	return nil
}

func (c *bitmapCache) RemoveFromStringBitmap(ctx context.Context, name string, value string, id uint64) (err error) {
	k := nameValue[string]{name: name, value: value}
	bitmap, ok := c.stringBitmaps[k]
	if !ok {
		bitmap, err = c.st.ReconstructLatestStringBitmap(ctx, name, value)
		if err != nil {
			return fmt.Errorf("failed to reconstruct string bitmap %q=%q: %w", name, value, err)
		}
		c.stringOldBitmaps[k] = cloneBitmap(bitmap)
		c.stringBitmaps[k] = bitmap
	}

	bitmap.Remove(id)
	return nil
}

func (c *bitmapCache) AddToNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.ReconstructLatestNumericBitmap(ctx, name, value)
		if err != nil {
			return fmt.Errorf("failed to reconstruct numeric bitmap %q=%d: %w", name, value, err)
		}
		c.numericOldBitmaps[k] = cloneBitmap(bitmap)
		c.numericBitmaps[k] = bitmap
	}

	bitmap.Add(id)
	return nil
}

func (c *bitmapCache) RemoveFromNumericBitmap(ctx context.Context, name string, value uint64, id uint64) (err error) {
	k := nameValue[uint64]{name: name, value: value}
	bitmap, ok := c.numericBitmaps[k]
	if !ok {
		bitmap, err = c.st.ReconstructLatestNumericBitmap(ctx, name, value)
		if err != nil {
			return fmt.Errorf("failed to reconstruct numeric bitmap %q=%d: %w", name, value, err)
		}
		c.numericOldBitmaps[k] = cloneBitmap(bitmap)
		c.numericBitmaps[k] = bitmap
	}

	bitmap.Remove(id)
	return nil
}

func (c *bitmapCache) Flush(ctx context.Context) (err error) {
	// RunOptimize all bitmaps in parallel (CPU-bound).
	eg := &errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())

	for _, bitmap := range c.stringBitmaps {
		if !bitmap.IsEmpty() {
			eg.Go(func() error {
				bitmap.RunOptimize()
				return nil
			})
		}
	}

	for _, bitmap := range c.numericBitmaps {
		if !bitmap.IsEmpty() {
			eg.Go(func() error {
				bitmap.RunOptimize()
				return nil
			})
		}
	}

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to run optimize: %w", err)
	}

	// Write string bitmaps as keyframes or deltas.
	for k, newBitmap := range c.stringBitmaps {
		oldBitmap := c.stringOldBitmaps[k]
		if oldBitmap == nil {
			oldBitmap = store.NewBitmap()
		}

		// Skip if bitmap hasn't changed.
		if newBitmap.Bitmap.Equals(oldBitmap.Bitmap) {
			continue
		}

		deltaCount, err := c.st.CountDeltasSinceLastKeyframeString(ctx, store.CountDeltasSinceLastKeyframeStringParams{
			Name: k.name, Value: k.value,
		})
		if err != nil {
			return fmt.Errorf("failed to count deltas for string %q=%q: %w", k.name, k.value, err)
		}

		isKeyframe := deltaCount >= store.KeyframeInterval || oldBitmap.IsEmpty()

		if isKeyframe {
			err = c.st.InsertStringBitmapEntry(ctx, store.InsertStringBitmapEntryParams{
				Name:         k.name,
				Value:        k.value,
				Block:        c.block,
				IsFullBitmap: true,
				Bitmap:       newBitmap,
			})
		} else {
			delta := store.ComputeDelta(oldBitmap.Bitmap, newBitmap.Bitmap)
			err = c.st.InsertStringBitmapEntry(ctx, store.InsertStringBitmapEntryParams{
				Name:         k.name,
				Value:        k.value,
				Block:        c.block,
				IsFullBitmap: false,
				Bitmap:       delta,
			})
		}
		if err != nil {
			return fmt.Errorf("failed to insert string bitmap entry %q=%q: %w", k.name, k.value, err)
		}
	}

	// Write numeric bitmaps as keyframes or deltas.
	for k, newBitmap := range c.numericBitmaps {
		oldBitmap := c.numericOldBitmaps[k]
		if oldBitmap == nil {
			oldBitmap = store.NewBitmap()
		}

		if newBitmap.Bitmap.Equals(oldBitmap.Bitmap) {
			continue
		}

		deltaCount, err := c.st.CountDeltasSinceLastKeyframeNumeric(ctx, store.CountDeltasSinceLastKeyframeNumericParams{
			Name: k.name, Value: k.value,
		})
		if err != nil {
			return fmt.Errorf("failed to count deltas for numeric %q=%d: %w", k.name, k.value, err)
		}

		isKeyframe := deltaCount >= store.KeyframeInterval || oldBitmap.IsEmpty()

		if isKeyframe {
			err = c.st.InsertNumericBitmapEntry(ctx, store.InsertNumericBitmapEntryParams{
				Name:         k.name,
				Value:        k.value,
				Block:        c.block,
				IsFullBitmap: true,
				Bitmap:       newBitmap,
			})
		} else {
			delta := store.ComputeDelta(oldBitmap.Bitmap, newBitmap.Bitmap)
			err = c.st.InsertNumericBitmapEntry(ctx, store.InsertNumericBitmapEntryParams{
				Name:         k.name,
				Value:        k.value,
				Block:        c.block,
				IsFullBitmap: false,
				Bitmap:       delta,
			})
		}
		if err != nil {
			return fmt.Errorf("failed to insert numeric bitmap entry %q=%d: %w", k.name, k.value, err)
		}
	}

	return nil
}
