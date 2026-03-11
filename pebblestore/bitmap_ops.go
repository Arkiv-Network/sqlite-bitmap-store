package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

// decodeBitmapValue parses the on-disk bitmap value format.
// Layout: [isKeyframe:1 byte][bitmapBytes...]
// isKeyframe is 0x01 for a keyframe, 0x00 for a delta.
func decodeBitmapValue(data []byte) (isKeyframe bool, bm *store.Bitmap, err error) {
	if len(data) < 1 {
		return false, nil, fmt.Errorf("pebblestore: bitmap value too short (%d bytes)", len(data))
	}
	isKeyframe = data[0] == 0x01
	bm = store.NewBitmap()
	if err := bm.Bitmap.UnmarshalBinary(data[1:]); err != nil {
		return false, nil, fmt.Errorf("pebblestore: unmarshal bitmap: %w", err)
	}
	return isKeyframe, bm, nil
}

// encodeBitmapValue serializes a bitmap with the isKeyframe prefix byte.
func encodeBitmapValue(isKeyframe bool, bm *store.Bitmap) ([]byte, error) {
	buf := new(bytes.Buffer)
	if isKeyframe {
		buf.WriteByte(0x01)
	} else {
		buf.WriteByte(0x00)
	}
	if _, err := bm.Bitmap.WriteTo(buf); err != nil {
		return nil, fmt.Errorf("pebblestore: serialize bitmap: %w", err)
	}
	return buf.Bytes(), nil
}

// InsertStringBitmapEntry writes a string bitmap entry (keyframe or delta) into
// the provided batch.
func (s *PebbleStore) InsertStringBitmapEntry(batch *pebble.Batch, name, value string, block uint64, isKeyframe bool, bm *store.Bitmap) error {
	encoded, err := encodeBitmapValue(isKeyframe, bm)
	if err != nil {
		return err
	}
	key := stringBitmapKey(name, value, block)
	return batch.Set(key, encoded, pebble.Sync)
}

// InsertNumericBitmapEntry writes a numeric bitmap entry (keyframe or delta)
// into the provided batch.
func (s *PebbleStore) InsertNumericBitmapEntry(batch *pebble.Batch, name string, value, block uint64, isKeyframe bool, bm *store.Bitmap) error {
	encoded, err := encodeBitmapValue(isKeyframe, bm)
	if err != nil {
		return err
	}
	key := numericBitmapKey(name, value, block)
	return batch.Set(key, encoded, pebble.Sync)
}

// ReconstructLatestStringBitmap reconstructs the current (latest) state of a
// string attribute bitmap by finding the latest keyframe and applying all
// subsequent deltas.
func (s *PebbleStore) ReconstructLatestStringBitmap(reader pebble.Reader, name, value string) (*store.Bitmap, error) {
	prefix := stringBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	// Reverse-iterate to find the latest keyframe.
	var keyframeBlock uint64
	found := false
	for iter.Last(); iter.Valid(); iter.Prev() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		if len(val) < 1 {
			continue
		}
		if val[0] == 0x01 { // keyframe
			_, _, block := parseStringBitmapKey(iter.Key())
			keyframeBlock = block
			found = true
			break
		}
	}

	if !found {
		return store.NewBitmap(), nil
	}

	// Forward-scan from the keyframe block to the end to build the chain.
	startKey := stringBitmapKey(name, value, keyframeBlock)
	var chain []store.BitmapChainEntry
	iter.SeekGE(startKey)
	for ; iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		isKF, bm, err := decodeBitmapValue(val)
		if err != nil {
			return nil, err
		}
		_, _, block := parseStringBitmapKey(iter.Key())
		chain = append(chain, store.BitmapChainEntry{
			Block:        block,
			IsFullBitmap: isKF,
			Bitmap:       bm,
		})
	}

	return store.ReconstructBitmap(chain), nil
}

// ReconstructLatestNumericBitmap reconstructs the current (latest) state of a
// numeric attribute bitmap by finding the latest keyframe and applying all
// subsequent deltas.
func (s *PebbleStore) ReconstructLatestNumericBitmap(reader pebble.Reader, name string, value uint64) (*store.Bitmap, error) {
	prefix := numericBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	// Reverse-iterate to find the latest keyframe.
	var keyframeBlock uint64
	found := false
	for iter.Last(); iter.Valid(); iter.Prev() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		if len(val) < 1 {
			continue
		}
		if val[0] == 0x01 { // keyframe
			_, _, block := parseNumericBitmapKey(iter.Key())
			keyframeBlock = block
			found = true
			break
		}
	}

	if !found {
		return store.NewBitmap(), nil
	}

	// Forward-scan from the keyframe block to the end to build the chain.
	startKey := numericBitmapKey(name, value, keyframeBlock)
	var chain []store.BitmapChainEntry
	iter.SeekGE(startKey)
	for ; iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		isKF, bm, err := decodeBitmapValue(val)
		if err != nil {
			return nil, err
		}
		_, _, block := parseNumericBitmapKey(iter.Key())
		chain = append(chain, store.BitmapChainEntry{
			Block:        block,
			IsFullBitmap: isKF,
			Bitmap:       bm,
		})
	}

	return store.ReconstructBitmap(chain), nil
}

// ReconstructStringBitmapAtBlock reconstructs the string attribute bitmap at a
// specific target block. If block is 0, it returns the latest state.
func (s *PebbleStore) ReconstructStringBitmapAtBlock(ctx context.Context, reader pebble.Reader, name, value string, block uint64) (*store.Bitmap, error) {
	if block == 0 {
		return s.ReconstructLatestStringBitmap(reader, name, value)
	}

	prefix := stringBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	// Find the keyframe at or before `block` by seeking to `block` and then
	// scanning backward.
	targetKey := stringBitmapKey(name, value, block)
	var keyframeBlock uint64
	found := false

	// SeekLT positions just before targetKey; we want inclusive, so we first
	// try SeekGE to land on the exact block if it exists.
	iter.SeekGE(targetKey)
	if iter.Valid() {
		_, _, b := parseStringBitmapKey(iter.Key())
		if b == block {
			// Check if this exact block entry is a keyframe or scan backward.
			val, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("pebblestore: read value: %w", err)
			}
			if len(val) > 0 && val[0] == 0x01 {
				keyframeBlock = block
				found = true
			}
		}
	}

	if !found {
		// Reverse-scan from the target block backward to find a keyframe.
		// Position at the target block key and walk backward.
		targetKeyNext := stringBitmapKey(name, value, block+1)
		iter.SeekGE(targetKeyNext)
		// Now Prev() moves to the entry at or before `block`.
		if !iter.Valid() {
			// Past the end; start from the last entry.
			iter.Last()
		} else {
			iter.Prev()
		}
		for ; iter.Valid(); iter.Prev() {
			// Verify we are still within the prefix bounds.
			k := iter.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			val, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("pebblestore: read value: %w", err)
			}
			if len(val) > 0 && val[0] == 0x01 {
				_, _, b := parseStringBitmapKey(k)
				keyframeBlock = b
				found = true
				break
			}
		}
	}

	if !found {
		return store.NewBitmap(), nil
	}

	// Forward-scan from keyframe block to target block (inclusive).
	startKey := stringBitmapKey(name, value, keyframeBlock)
	// Build an upper bound that is one block past the target so the scan
	// includes the target block entry.
	scanUpper := stringBitmapKey(name, value, block+1)

	iter2, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: scanUpper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create forward iterator: %w", err)
	}
	defer iter2.Close()

	var chain []store.BitmapChainEntry
	for iter2.First(); iter2.Valid(); iter2.Next() {
		val, err := iter2.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		isKF, bm, err := decodeBitmapValue(val)
		if err != nil {
			return nil, err
		}
		_, _, b := parseStringBitmapKey(iter2.Key())
		chain = append(chain, store.BitmapChainEntry{
			Block:        b,
			IsFullBitmap: isKF,
			Bitmap:       bm,
		})
	}

	return store.ReconstructBitmap(chain), nil
}

// ReconstructNumericBitmapAtBlock reconstructs the numeric attribute bitmap at
// a specific target block. If block is 0, it returns the latest state.
func (s *PebbleStore) ReconstructNumericBitmapAtBlock(ctx context.Context, reader pebble.Reader, name string, value, block uint64) (*store.Bitmap, error) {
	if block == 0 {
		return s.ReconstructLatestNumericBitmap(reader, name, value)
	}

	prefix := numericBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	// Find the keyframe at or before `block`.
	targetKey := numericBitmapKey(name, value, block)
	var keyframeBlock uint64
	found := false

	iter.SeekGE(targetKey)
	if iter.Valid() {
		_, _, b := parseNumericBitmapKey(iter.Key())
		if b == block {
			val, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("pebblestore: read value: %w", err)
			}
			if len(val) > 0 && val[0] == 0x01 {
				keyframeBlock = block
				found = true
			}
		}
	}

	if !found {
		targetKeyNext := numericBitmapKey(name, value, block+1)
		iter.SeekGE(targetKeyNext)
		if !iter.Valid() {
			iter.Last()
		} else {
			iter.Prev()
		}
		for ; iter.Valid(); iter.Prev() {
			k := iter.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			val, err := iter.ValueAndErr()
			if err != nil {
				return nil, fmt.Errorf("pebblestore: read value: %w", err)
			}
			if len(val) > 0 && val[0] == 0x01 {
				_, _, b := parseNumericBitmapKey(k)
				keyframeBlock = b
				found = true
				break
			}
		}
	}

	if !found {
		return store.NewBitmap(), nil
	}

	startKey := numericBitmapKey(name, value, keyframeBlock)
	scanUpper := numericBitmapKey(name, value, block+1)

	iter2, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: scanUpper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create forward iterator: %w", err)
	}
	defer iter2.Close()

	var chain []store.BitmapChainEntry
	for iter2.First(); iter2.Valid(); iter2.Next() {
		val, err := iter2.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		isKF, bm, err := decodeBitmapValue(val)
		if err != nil {
			return nil, err
		}
		_, _, b := parseNumericBitmapKey(iter2.Key())
		chain = append(chain, store.BitmapChainEntry{
			Block:        b,
			IsFullBitmap: isKF,
			Bitmap:       bm,
		})
	}

	return store.ReconstructBitmap(chain), nil
}

// CountDeltasSinceLastKeyframeString counts the number of delta entries that
// follow the most recent keyframe for a (name, value) pair in the string bitmap
// index. If no keyframe is found, it returns the total number of entries.
func (s *PebbleStore) CountDeltasSinceLastKeyframeString(reader pebble.Reader, name, value string) (int64, error) {
	prefix := stringBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.Last(); iter.Valid(); iter.Prev() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return 0, fmt.Errorf("pebblestore: read value: %w", err)
		}
		if len(val) > 0 && val[0] == 0x01 {
			// Found a keyframe; stop counting.
			break
		}
		count++
	}

	return count, nil
}

// CountDeltasSinceLastKeyframeNumeric counts the number of delta entries that
// follow the most recent keyframe for a (name, value) pair in the numeric
// bitmap index. If no keyframe is found, it returns the total number of entries.
func (s *PebbleStore) CountDeltasSinceLastKeyframeNumeric(reader pebble.Reader, name string, value uint64) (int64, error) {
	prefix := numericBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.Last(); iter.Valid(); iter.Prev() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return 0, fmt.Errorf("pebblestore: read value: %w", err)
		}
		if len(val) > 0 && val[0] == 0x01 {
			// Found a keyframe; stop counting.
			break
		}
		count++
	}

	return count, nil
}

// extractBlockFromStringKey extracts the block number from the trailing 8 bytes
// of a string bitmap key. This is a convenience wrapper that avoids parsing the
// full key when only the block is needed.
func extractBlockFromStringKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

// extractBlockFromNumericKey extracts the block number from the trailing 8
// bytes of a numeric bitmap key.
func extractBlockFromNumericKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(key)-8:])
}
