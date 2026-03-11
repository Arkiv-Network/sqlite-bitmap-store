package pebblestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"
)

// EvaluateAllCurrent returns the IDs of all entities that have a current
// (active) payload, sorted by ID descending. It scans all keys with the
// entity-current prefix (0x03).
func (s *PebbleStore) EvaluateAllCurrent(ctx context.Context, reader pebble.Reader) ([]uint64, error) {
	lower := []byte{prefixEntityCurrent}
	upper := []byte{prefixEntityCurrent + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var ids []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}
		id := binary.BigEndian.Uint64(val)
		ids = append(ids, id)
	}

	// Sort descending to match SQLite's ORDER BY id DESC.
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] > ids[j]
	})

	return ids, nil
}

// EvaluateAllAtBlock returns the IDs of all entities that were active at the
// given block number, sorted by ID descending. An entity is considered active
// at a block if its fromBlock <= block and either its toBlock is null (still
// active) or its toBlock > block.
func (s *PebbleStore) EvaluateAllAtBlock(ctx context.Context, reader pebble.Reader, block uint64) ([]uint64, error) {
	lower := fromBlockIndexKey(0, 0)
	upper := fromBlockIndexKey(block+1, 0)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var ids []uint64
	for iter.First(); iter.Valid(); iter.Next() {
		_, id := parseFromBlockIndexKey(iter.Key())

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebblestore: read value: %w", err)
		}

		// Value layout: [toBlock:8BE][toBlockIsNull:1]
		toBlock := binary.BigEndian.Uint64(val[:8])
		toBlockIsNull := val[8] == 0x01

		if toBlockIsNull || toBlock > block {
			ids = append(ids, id)
		}
	}

	// Sort descending to match SQLite's ORDER BY id DESC.
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] > ids[j]
	})

	return ids, nil
}

// GetNumberOfEntities returns the number of entities that currently have an
// active payload by counting all keys with the entity-current prefix (0x03).
func (s *PebbleStore) GetNumberOfEntities(ctx context.Context, reader pebble.Reader) (int64, error) {
	lower := []byte{prefixEntityCurrent}
	upper := []byte{prefixEntityCurrent + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, nil
}
