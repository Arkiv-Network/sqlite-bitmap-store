package store

import (
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
