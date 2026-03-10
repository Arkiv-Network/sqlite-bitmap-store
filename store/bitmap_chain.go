package store

import (
	"context"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// KeyframeInterval is the maximum number of delta frames before a new keyframe
// is stored. Analogous to constant C in the bitemporality proposal.
const KeyframeInterval int64 = 128

// BitmapChainEntry represents one entry in a keyframe/delta chain.
type BitmapChainEntry struct {
	Block        uint64
	IsFullBitmap bool
	Bitmap       *Bitmap
}

// ReconstructBitmap takes a chain of entries (keyframe first, then deltas in
// block order) and reconstructs the bitmap state at the last entry's block.
// If the chain is empty, returns an empty bitmap.
func ReconstructBitmap(chain []BitmapChainEntry) *Bitmap {
	if len(chain) == 0 {
		return NewBitmap()
	}

	result := NewBitmap()
	if chain[0].Bitmap != nil && chain[0].Bitmap.Bitmap != nil {
		result.Bitmap = chain[0].Bitmap.Bitmap.Clone()
	}

	for _, entry := range chain[1:] {
		if entry.Bitmap != nil && entry.Bitmap.Bitmap != nil {
			result.Bitmap.Xor(entry.Bitmap.Bitmap)
		}
	}

	return result
}

// coalesceBlockToInt64 extracts an int64 from the interface{} returned by
// COALESCE(MAX(block), -1) queries. Returns -1 if the value is nil.
func coalesceBlockToInt64(v interface{}) (int64, error) {
	if v == nil {
		return -1, nil
	}
	switch val := v.(type) {
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	default:
		return -1, fmt.Errorf("unexpected type for block value: %T", v)
	}
}

// ReconstructLatestStringBitmap reconstructs the current (latest) state of a
// string attribute bitmap by loading the full chain from the latest keyframe.
func (q *Queries) ReconstructLatestStringBitmap(ctx context.Context, name, value string) (*Bitmap, error) {
	kfBlockRaw, err := q.GetLatestStringKeyframeBlock(ctx, GetLatestStringKeyframeBlockParams{
		Name: name, Value: value,
	})
	if err != nil {
		return nil, fmt.Errorf("get latest string keyframe block: %w", err)
	}

	kfBlock, err := coalesceBlockToInt64(kfBlockRaw)
	if err != nil {
		return nil, err
	}
	if kfBlock < 0 {
		return NewBitmap(), nil
	}

	rows, err := q.GetAllStringBitmapEntriesFrom(ctx, GetAllStringBitmapEntriesFromParams{
		Name: name, Value: value, FromBlock: uint64(kfBlock),
	})
	if err != nil {
		return nil, fmt.Errorf("get string bitmap entries: %w", err)
	}

	chain := make([]BitmapChainEntry, len(rows))
	for i, r := range rows {
		chain[i] = BitmapChainEntry{Block: r.Block, IsFullBitmap: r.IsFullBitmap, Bitmap: r.Bitmap}
	}
	return ReconstructBitmap(chain), nil
}

// ReconstructLatestNumericBitmap reconstructs the current (latest) state of a
// numeric attribute bitmap.
func (q *Queries) ReconstructLatestNumericBitmap(ctx context.Context, name string, value uint64) (*Bitmap, error) {
	kfBlockRaw, err := q.GetLatestNumericKeyframeBlock(ctx, GetLatestNumericKeyframeBlockParams{
		Name: name, Value: value,
	})
	if err != nil {
		return nil, fmt.Errorf("get latest numeric keyframe block: %w", err)
	}

	kfBlock, err := coalesceBlockToInt64(kfBlockRaw)
	if err != nil {
		return nil, err
	}
	if kfBlock < 0 {
		return NewBitmap(), nil
	}

	rows, err := q.GetAllNumericBitmapEntriesFrom(ctx, GetAllNumericBitmapEntriesFromParams{
		Name: name, Value: value, FromBlock: uint64(kfBlock),
	})
	if err != nil {
		return nil, fmt.Errorf("get numeric bitmap entries: %w", err)
	}

	chain := make([]BitmapChainEntry, len(rows))
	for i, r := range rows {
		chain[i] = BitmapChainEntry{Block: r.Block, IsFullBitmap: r.IsFullBitmap, Bitmap: r.Bitmap}
	}
	return ReconstructBitmap(chain), nil
}

// ReconstructStringBitmapAtBlock reconstructs the string attribute bitmap at a
// specific target block. If block is 0, returns the latest state.
func (q *Queries) ReconstructStringBitmapAtBlock(ctx context.Context, name, value string, block uint64) (*Bitmap, error) {
	if block == 0 {
		return q.ReconstructLatestStringBitmap(ctx, name, value)
	}

	kfBlockRaw, err := q.GetStringKeyframeBlockAtOrBefore(ctx, GetStringKeyframeBlockAtOrBeforeParams{
		Name: name, Value: value, TargetBlock: block,
	})
	if err != nil {
		return nil, fmt.Errorf("get string keyframe at or before block %d: %w", block, err)
	}

	kfBlock, err := coalesceBlockToInt64(kfBlockRaw)
	if err != nil {
		return nil, err
	}
	if kfBlock < 0 {
		return NewBitmap(), nil
	}

	rows, err := q.GetStringBitmapEntriesInRange(ctx, GetStringBitmapEntriesInRangeParams{
		Name: name, Value: value, FromBlock: uint64(kfBlock), ToBlock: block,
	})
	if err != nil {
		return nil, fmt.Errorf("get string bitmap entries in range: %w", err)
	}

	chain := make([]BitmapChainEntry, len(rows))
	for i, r := range rows {
		chain[i] = BitmapChainEntry{Block: r.Block, IsFullBitmap: r.IsFullBitmap, Bitmap: r.Bitmap}
	}
	return ReconstructBitmap(chain), nil
}

// ReconstructNumericBitmapAtBlock reconstructs the numeric attribute bitmap at
// a specific target block. If block is 0, returns the latest state.
func (q *Queries) ReconstructNumericBitmapAtBlock(ctx context.Context, name string, value, block uint64) (*Bitmap, error) {
	if block == 0 {
		return q.ReconstructLatestNumericBitmap(ctx, name, value)
	}

	kfBlockRaw, err := q.GetNumericKeyframeBlockAtOrBefore(ctx, GetNumericKeyframeBlockAtOrBeforeParams{
		Name: name, Value: value, TargetBlock: block,
	})
	if err != nil {
		return nil, fmt.Errorf("get numeric keyframe at or before block %d: %w", block, err)
	}

	kfBlock, err := coalesceBlockToInt64(kfBlockRaw)
	if err != nil {
		return nil, err
	}
	if kfBlock < 0 {
		return NewBitmap(), nil
	}

	rows, err := q.GetNumericBitmapEntriesInRange(ctx, GetNumericBitmapEntriesInRangeParams{
		Name: name, Value: value, FromBlock: uint64(kfBlock), ToBlock: block,
	})
	if err != nil {
		return nil, fmt.Errorf("get numeric bitmap entries in range: %w", err)
	}

	chain := make([]BitmapChainEntry, len(rows))
	for i, r := range rows {
		chain[i] = BitmapChainEntry{Block: r.Block, IsFullBitmap: r.IsFullBitmap, Bitmap: r.Bitmap}
	}
	return ReconstructBitmap(chain), nil
}

// EffectiveBlock returns the block value to use in queries. If block is 0
// (meaning "latest"), returns math.MaxUint64 so that all entries match.
func EffectiveBlock(block uint64) uint64 {
	if block == 0 {
		return math.MaxUint64
	}
	return block
}

// ComputeDelta computes the XOR of two bitmaps. The result, when XOR'd with
// oldBitmap, produces newBitmap.
func ComputeDelta(oldBitmap, newBitmap *roaring64.Bitmap) *Bitmap {
	delta := NewBitmap()
	delta.Bitmap = roaring64.Xor(oldBitmap, newBitmap)
	delta.RunOptimize()
	return delta
}
