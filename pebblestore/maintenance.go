package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// PruneBefore removes all payloads with toBlock <= block and prunes bitmap
// entries that are superseded by a keyframe at or before block. All mutations
// are performed in a single IndexedBatch committed with pebble.Sync.
func (s *PebbleStore) PruneBefore(ctx context.Context, block uint64) error {
	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	if err := s.prunePayloads(batch, block); err != nil {
		return fmt.Errorf("pebblestore: prune payloads: %w", err)
	}

	if err := s.pruneStringBitmaps(batch, block); err != nil {
		return fmt.Errorf("pebblestore: prune string bitmaps: %w", err)
	}

	if err := s.pruneNumericBitmaps(batch, block); err != nil {
		return fmt.Errorf("pebblestore: prune numeric bitmaps: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// prunePayloads deletes all payloads whose toBlock <= block, along with their
// associated from-block and to-block index entries.
func (s *PebbleStore) prunePayloads(batch *pebble.Batch, block uint64) error {
	// Scan 0x05 keys from [0x05][0:8BE][0:8BE] to [0x05][block:8BE][0xFF...0xFF] inclusive.
	lower := toBlockIndexKey(0, 0)

	// Upper bound: block+1 at id 0 gives us everything up to and including block.
	var upper []byte
	if block < ^uint64(0) {
		upper = toBlockIndexKey(block+1, 0)
	} else {
		// block is max uint64; scan the entire 0x05 prefix.
		upper = prefixUpperBound([]byte{prefixToBlockIndex})
	}

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create to-block iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		toBlock, id := parseToBlockIndexKey(iter.Key())
		if toBlock > block {
			break
		}

		// Read the payload to get the fromBlock for the from-block index key.
		pKey := payloadKey(id)
		payloadVal, closer, err := batch.Get(pKey)
		if err != nil {
			return fmt.Errorf("get payload %d for pruning: %w", id, err)
		}
		data := make([]byte, len(payloadVal))
		copy(data, payloadVal)
		closer.Close()

		d, err := decodePayloadValue(data)
		if err != nil {
			return fmt.Errorf("decode payload %d: %w", id, err)
		}

		// Delete 0x02 payload.
		if err := batch.Delete(pKey, pebble.Sync); err != nil {
			return fmt.Errorf("delete payload %d: %w", id, err)
		}

		// Delete 0x04 from-block index.
		if err := batch.Delete(fromBlockIndexKey(d.FromBlock, id), pebble.Sync); err != nil {
			return fmt.Errorf("delete from-block index for %d: %w", id, err)
		}

		// Delete 0x05 to-block index.
		if err := batch.Delete(toBlockIndexKey(toBlock, id), pebble.Sync); err != nil {
			return fmt.Errorf("delete to-block index for %d: %w", id, err)
		}
	}

	return iter.Error()
}

// pruneStringBitmaps removes obsolete string bitmap entries. For each unique
// (name, value) pair, it finds the latest keyframe at or before block and
// deletes all entries with block number strictly less than that keyframe.
func (s *PebbleStore) pruneStringBitmaps(batch *pebble.Batch, block uint64) error {
	prefix := []byte{prefixStringBitmap}
	upper := prefixUpperBound(prefix)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create string bitmap iterator: %w", err)
	}
	defer iter.Close()

	type stringPair struct {
		name, value string
	}
	seen := make(map[stringPair]struct{})

	for iter.First(); iter.Valid(); iter.Next() {
		name, value, _ := parseStringBitmapKey(iter.Key())
		pair := stringPair{name, value}
		if _, ok := seen[pair]; ok {
			continue
		}
		seen[pair] = struct{}{}

		keyframeBlock, found, err := s.findLatestStringKeyframeBefore(batch, name, value, block)
		if err != nil {
			return fmt.Errorf("find string keyframe for %q=%q: %w", name, value, err)
		}
		if !found {
			continue
		}

		if err := s.deleteStringBitmapsBefore(batch, name, value, keyframeBlock); err != nil {
			return fmt.Errorf("delete string bitmaps for %q=%q: %w", name, value, err)
		}
	}

	return iter.Error()
}

// findLatestStringKeyframeBefore finds the latest keyframe block number at or
// before the given block for a (name, value) pair in the string bitmap index.
func (s *PebbleStore) findLatestStringKeyframeBefore(reader pebble.Reader, name, value string, block uint64) (keyframeBlock uint64, found bool, err error) {
	prefix := stringBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return 0, false, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	// Position just past block and walk backward to find a keyframe at or
	// before the target block.
	targetKey := stringBitmapKey(name, value, block+1)
	iter.SeekLT(targetKey)
	for ; iter.Valid(); iter.Prev() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return 0, false, fmt.Errorf("read value: %w", err)
		}
		if len(val) > 0 && val[0] == 0x01 {
			_, _, b := parseStringBitmapKey(k)
			return b, true, nil
		}
	}

	return 0, false, iter.Error()
}

// deleteStringBitmapsBefore deletes all string bitmap entries for (name, value)
// with block number strictly less than keyframeBlock.
func (s *PebbleStore) deleteStringBitmapsBefore(batch *pebble.Batch, name, value string, keyframeBlock uint64) error {
	prefix := stringBitmapNameValuePrefix(name, value)
	upperKey := stringBitmapKey(name, value, keyframeBlock)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperKey,
	})
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		keyToDelete := make([]byte, len(iter.Key()))
		copy(keyToDelete, iter.Key())
		if err := batch.Delete(keyToDelete, pebble.Sync); err != nil {
			return fmt.Errorf("delete key: %w", err)
		}
	}

	return iter.Error()
}

// pruneNumericBitmaps removes obsolete numeric bitmap entries using the same
// strategy as pruneStringBitmaps.
func (s *PebbleStore) pruneNumericBitmaps(batch *pebble.Batch, block uint64) error {
	prefix := []byte{prefixNumericBitmap}
	upper := prefixUpperBound(prefix)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create numeric bitmap iterator: %w", err)
	}
	defer iter.Close()

	type numericPair struct {
		name  string
		value uint64
	}
	seen := make(map[numericPair]struct{})

	for iter.First(); iter.Valid(); iter.Next() {
		name, value, _ := parseNumericBitmapKey(iter.Key())
		pair := numericPair{name, value}
		if _, ok := seen[pair]; ok {
			continue
		}
		seen[pair] = struct{}{}

		keyframeBlock, found, err := s.findLatestNumericKeyframeBefore(batch, name, value, block)
		if err != nil {
			return fmt.Errorf("find numeric keyframe for %q=%d: %w", name, value, err)
		}
		if !found {
			continue
		}

		if err := s.deleteNumericBitmapsBefore(batch, name, value, keyframeBlock); err != nil {
			return fmt.Errorf("delete numeric bitmaps for %q=%d: %w", name, value, err)
		}
	}

	return iter.Error()
}

// findLatestNumericKeyframeBefore finds the latest keyframe block number at or
// before the given block for a (name, value) pair in the numeric bitmap index.
func (s *PebbleStore) findLatestNumericKeyframeBefore(reader pebble.Reader, name string, value uint64, block uint64) (keyframeBlock uint64, found bool, err error) {
	prefix := numericBitmapNameValuePrefix(name, value)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return 0, false, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	targetKey := numericBitmapKey(name, value, block+1)
	iter.SeekLT(targetKey)
	for ; iter.Valid(); iter.Prev() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return 0, false, fmt.Errorf("read value: %w", err)
		}
		if len(val) > 0 && val[0] == 0x01 {
			_, _, b := parseNumericBitmapKey(k)
			return b, true, nil
		}
	}

	return 0, false, iter.Error()
}

// deleteNumericBitmapsBefore deletes all numeric bitmap entries for
// (name, value) with block number strictly less than keyframeBlock.
func (s *PebbleStore) deleteNumericBitmapsBefore(batch *pebble.Batch, name string, value uint64, keyframeBlock uint64) error {
	prefix := numericBitmapNameValuePrefix(name, value)
	upperKey := numericBitmapKey(name, value, keyframeBlock)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperKey,
	})
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		keyToDelete := make([]byte, len(iter.Key()))
		copy(keyToDelete, iter.Key())
		if err := batch.Delete(keyToDelete, pebble.Sync); err != nil {
			return fmt.Errorf("delete key: %w", err)
		}
	}

	return iter.Error()
}

// HandleReorg rolls back state to the given block. It reopens payloads that
// were closed after block, deletes payloads created after block, removes
// bitmap entries after block, and updates the last_block marker.
func (s *PebbleStore) HandleReorg(ctx context.Context, block uint64) error {
	batch := s.db.NewIndexedBatch()
	defer batch.Close()

	if err := s.reopenPayloadsClosedAfter(batch, block); err != nil {
		return fmt.Errorf("pebblestore: reopen payloads: %w", err)
	}

	if err := s.deletePayloadsCreatedAfter(batch, block); err != nil {
		return fmt.Errorf("pebblestore: delete payloads: %w", err)
	}

	if err := s.deleteBitmapEntriesAfter(batch, block); err != nil {
		return fmt.Errorf("pebblestore: delete bitmap entries: %w", err)
	}

	if err := s.UpsertLastBlock(batch, block); err != nil {
		return fmt.Errorf("pebblestore: update last block: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// reopenPayloadsClosedAfter scans 0x05 keys from (block+1, 0) onward and
// reopens each payload by setting toBlock=0, toBlockIsNull=true and
// restoring the entity-current pointer.
func (s *PebbleStore) reopenPayloadsClosedAfter(batch *pebble.Batch, block uint64) error {
	lower := toBlockIndexKey(block+1, 0)
	upper := prefixUpperBound([]byte{prefixToBlockIndex})

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create to-block iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, id := parseToBlockIndexKey(iter.Key())

		// Read payload to decode and re-encode.
		pKey := payloadKey(id)
		payloadVal, closer, err := batch.Get(pKey)
		if err != nil {
			return fmt.Errorf("get payload %d: %w", id, err)
		}
		data := make([]byte, len(payloadVal))
		copy(data, payloadVal)
		closer.Close()

		d, err := decodePayloadValue(data)
		if err != nil {
			return fmt.Errorf("decode payload %d: %w", id, err)
		}

		// Re-encode with toBlock=0, toBlockIsNull=true (reopen).
		updatedVal, err := encodePayloadValue(
			d.FromBlock,
			0,    // toBlock
			true, // toBlockIsNull
			d.EntityKey,
			d.ContentType,
			d.StringAttributes,
			d.NumericAttributes,
			d.Payload,
		)
		if err != nil {
			return fmt.Errorf("re-encode payload %d: %w", id, err)
		}

		// Update 0x02 payload.
		if err := batch.Set(pKey, updatedVal, pebble.Sync); err != nil {
			return fmt.Errorf("set payload %d: %w", id, err)
		}

		// Re-insert 0x03 entity-current key.
		var idBuf [8]byte
		binary.BigEndian.PutUint64(idBuf[:], id)
		if err := batch.Set(entityCurrentKey(d.EntityKey), idBuf[:], pebble.Sync); err != nil {
			return fmt.Errorf("set entity current for payload %d: %w", id, err)
		}

		// Delete 0x05 to-block index key.
		toKey := make([]byte, len(iter.Key()))
		copy(toKey, iter.Key())
		if err := batch.Delete(toKey, pebble.Sync); err != nil {
			return fmt.Errorf("delete to-block index for %d: %w", id, err)
		}

		// Update 0x04 from-block index value: [toBlock=0:8BE][toBlockIsNull=0x01:1byte].
		var fromIdxVal [9]byte
		// toBlock = 0 is already zero-valued.
		fromIdxVal[8] = 0x01 // toBlockIsNull = true
		if err := batch.Set(fromBlockIndexKey(d.FromBlock, id), fromIdxVal[:], pebble.Sync); err != nil {
			return fmt.Errorf("update from-block index for %d: %w", id, err)
		}
	}

	return iter.Error()
}

// deletePayloadsCreatedAfter removes payloads whose fromBlock > block along
// with all their associated index keys.
func (s *PebbleStore) deletePayloadsCreatedAfter(batch *pebble.Batch, block uint64) error {
	lower := fromBlockIndexKey(block+1, 0)
	upper := prefixUpperBound([]byte{prefixFromBlockIndex})

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create from-block iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, id := parseFromBlockIndexKey(iter.Key())

		// Read 0x02 payload to get entityKey.
		pKey := payloadKey(id)
		payloadVal, closer, err := batch.Get(pKey)
		if err != nil {
			return fmt.Errorf("get payload %d: %w", id, err)
		}
		data := make([]byte, len(payloadVal))
		copy(data, payloadVal)
		closer.Close()

		d, err := decodePayloadValue(data)
		if err != nil {
			return fmt.Errorf("decode payload %d: %w", id, err)
		}

		// Delete 0x02 payload.
		if err := batch.Delete(pKey, pebble.Sync); err != nil {
			return fmt.Errorf("delete payload %d: %w", id, err)
		}

		// Delete 0x03 entity-current if it exists.
		ecKey := entityCurrentKey(d.EntityKey)
		_, closer, err = batch.Get(ecKey)
		if err == nil {
			closer.Close()
			if err := batch.Delete(ecKey, pebble.Sync); err != nil {
				return fmt.Errorf("delete entity current for %d: %w", id, err)
			}
		} else if err != pebble.ErrNotFound {
			return fmt.Errorf("check entity current for %d: %w", id, err)
		}

		// Delete 0x04 from-block index.
		fromKey := make([]byte, len(iter.Key()))
		copy(fromKey, iter.Key())
		if err := batch.Delete(fromKey, pebble.Sync); err != nil {
			return fmt.Errorf("delete from-block index for %d: %w", id, err)
		}

		// Delete 0x05 to-block index if it exists.
		if !d.ToBlockIsNull {
			toKey := toBlockIndexKey(d.ToBlock, id)
			_, closer, err = batch.Get(toKey)
			if err == nil {
				closer.Close()
				if err := batch.Delete(toKey, pebble.Sync); err != nil {
					return fmt.Errorf("delete to-block index for %d: %w", id, err)
				}
			} else if err != pebble.ErrNotFound {
				return fmt.Errorf("check to-block index for %d: %w", id, err)
			}
		}
	}

	return iter.Error()
}

// deleteBitmapEntriesAfter removes all string and numeric bitmap entries whose
// block number is greater than the given block.
func (s *PebbleStore) deleteBitmapEntriesAfter(batch *pebble.Batch, block uint64) error {
	// Delete string bitmap entries after block.
	if err := s.deleteStringBitmapEntriesAfter(batch, block); err != nil {
		return fmt.Errorf("string bitmaps: %w", err)
	}

	// Delete numeric bitmap entries after block.
	if err := s.deleteNumericBitmapEntriesAfter(batch, block); err != nil {
		return fmt.Errorf("numeric bitmaps: %w", err)
	}

	return nil
}

// deleteStringBitmapEntriesAfter scans all 0x10 keys and deletes those with
// block > the reorg block.
func (s *PebbleStore) deleteStringBitmapEntriesAfter(batch *pebble.Batch, block uint64) error {
	prefix := []byte{prefixStringBitmap}
	upper := prefixUpperBound(prefix)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, _, b := parseStringBitmapKey(iter.Key())
		if b > block {
			keyToDelete := make([]byte, len(iter.Key()))
			copy(keyToDelete, iter.Key())
			if err := batch.Delete(keyToDelete, pebble.Sync); err != nil {
				return fmt.Errorf("delete key: %w", err)
			}
		}
	}

	return iter.Error()
}

// deleteNumericBitmapEntriesAfter scans all 0x20 keys and deletes those with
// block > the reorg block.
func (s *PebbleStore) deleteNumericBitmapEntriesAfter(batch *pebble.Batch, block uint64) error {
	prefix := []byte{prefixNumericBitmap}
	upper := prefixUpperBound(prefix)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, _, b := parseNumericBitmapKey(iter.Key())
		if b > block {
			keyToDelete := make([]byte, len(iter.Key()))
			copy(keyToDelete, iter.Key())
			if err := batch.Delete(keyToDelete, pebble.Sync); err != nil {
				return fmt.Errorf("delete key: %w", err)
			}
		}
	}

	return iter.Error()
}
