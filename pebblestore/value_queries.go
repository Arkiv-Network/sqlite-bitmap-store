package pebblestore

import (
	"context"
	"fmt"
	"path"

	"github.com/cockroachdb/pebble"
)

// scanDistinctStringValues scans all string bitmap entries for the given name,
// skips entries where block > targetBlock, applies the filter function, and
// returns the distinct values that pass the filter.
func (s *PebbleStore) scanDistinctStringValues(reader pebble.Reader, name string, targetBlock uint64, filter func(value string) bool) ([]string, error) {
	prefix := stringBitmapPrefix(name)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	seen := make(map[string]struct{})
	var result []string

	for iter.First(); iter.Valid(); iter.Next() {
		_, value, block := parseStringBitmapKey(iter.Key())

		if block > targetBlock {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		if filter(value) {
			seen[value] = struct{}{}
			result = append(result, value)
		} else {
			// Mark as seen even if filtered out, so we don't re-evaluate.
			seen[value] = struct{}{}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebblestore: iterator error: %w", err)
	}

	return result, nil
}

// scanDistinctNumericValues scans all numeric bitmap entries for the given name,
// skips entries where block > targetBlock, applies the filter function, and
// returns the distinct values that pass the filter.
func (s *PebbleStore) scanDistinctNumericValues(reader pebble.Reader, name string, targetBlock uint64, filter func(value uint64) bool) ([]uint64, error) {
	prefix := numericBitmapPrefix(name)
	upper := prefixUpperBound(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: create iterator: %w", err)
	}
	defer iter.Close()

	seen := make(map[uint64]struct{})
	var result []uint64

	for iter.First(); iter.Valid(); iter.Next() {
		_, value, block := parseNumericBitmapKey(iter.Key())

		if block > targetBlock {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		if filter(value) {
			seen[value] = struct{}{}
			result = append(result, value)
		} else {
			seen[value] = struct{}{}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebblestore: iterator error: %w", err)
	}

	return result, nil
}

// --- String value query methods ---

// GetMatchingStringValuesEqual returns distinct string values for the given name
// where the value equals the provided value, considering only entries at or
// before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesEqual(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v == value
	})
}

// GetMatchingStringValuesNotEqual returns distinct string values for the given
// name where the value does not equal the provided value, considering only
// entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesNotEqual(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v != value
	})
}

// GetMatchingStringValuesLessThan returns distinct string values for the given
// name where the value is lexicographically less than the provided value,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesLessThan(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v < value
	})
}

// GetMatchingStringValuesGreaterThan returns distinct string values for the
// given name where the value is lexicographically greater than the provided
// value, considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesGreaterThan(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v > value
	})
}

// GetMatchingStringValuesLessOrEqualThan returns distinct string values for the
// given name where the value is lexicographically less than or equal to the
// provided value, considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesLessOrEqualThan(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v <= value
	})
}

// GetMatchingStringValuesGreaterOrEqualThan returns distinct string values for
// the given name where the value is lexicographically greater than or equal to
// the provided value, considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesGreaterOrEqualThan(ctx context.Context, reader pebble.Reader, name, value string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		return v >= value
	})
}

// GetMatchingStringValuesGlob returns distinct string values for the given name
// where the value matches the provided glob pattern, considering only entries at
// or before targetBlock. The pattern syntax is that of path.Match.
func (s *PebbleStore) GetMatchingStringValuesGlob(ctx context.Context, reader pebble.Reader, name, pattern string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		matched, _ := path.Match(pattern, v)
		return matched
	})
}

// GetMatchingStringValuesNotGlob returns distinct string values for the given
// name where the value does not match the provided glob pattern, considering
// only entries at or before targetBlock. The pattern syntax is that of
// path.Match.
func (s *PebbleStore) GetMatchingStringValuesNotGlob(ctx context.Context, reader pebble.Reader, name, pattern string, targetBlock uint64) ([]string, error) {
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		matched, _ := path.Match(pattern, v)
		return !matched
	})
}

// GetMatchingStringValuesInclusion returns distinct string values for the given
// name where the value is contained in the provided set of values, considering
// only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesInclusion(ctx context.Context, reader pebble.Reader, name string, values []string, targetBlock uint64) ([]string, error) {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		_, ok := set[v]
		return ok
	})
}

// GetMatchingStringValuesNotInclusion returns distinct string values for the
// given name where the value is not contained in the provided set of values,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingStringValuesNotInclusion(ctx context.Context, reader pebble.Reader, name string, values []string, targetBlock uint64) ([]string, error) {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return s.scanDistinctStringValues(reader, name, targetBlock, func(v string) bool {
		_, ok := set[v]
		return !ok
	})
}

// --- Numeric value query methods ---

// GetMatchingNumericValuesEqual returns distinct numeric values for the given
// name where the value equals the provided value, considering only entries at or
// before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesEqual(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v == value
	})
}

// GetMatchingNumericValuesNotEqual returns distinct numeric values for the given
// name where the value does not equal the provided value, considering only
// entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesNotEqual(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v != value
	})
}

// GetMatchingNumericValuesLessThan returns distinct numeric values for the given
// name where the value is less than the provided value, considering only entries
// at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesLessThan(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v < value
	})
}

// GetMatchingNumericValuesGreaterThan returns distinct numeric values for the
// given name where the value is greater than the provided value, considering
// only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesGreaterThan(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v > value
	})
}

// GetMatchingNumericValuesLessOrEqualThan returns distinct numeric values for
// the given name where the value is less than or equal to the provided value,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesLessOrEqualThan(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v <= value
	})
}

// GetMatchingNumericValuesGreaterOrEqualThan returns distinct numeric values for
// the given name where the value is greater than or equal to the provided value,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesGreaterOrEqualThan(ctx context.Context, reader pebble.Reader, name string, value, targetBlock uint64) ([]uint64, error) {
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		return v >= value
	})
}

// GetMatchingNumericValuesInclusion returns distinct numeric values for the
// given name where the value is contained in the provided set of values,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesInclusion(ctx context.Context, reader pebble.Reader, name string, values []uint64, targetBlock uint64) ([]uint64, error) {
	set := make(map[uint64]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		_, ok := set[v]
		return ok
	})
}

// GetMatchingNumericValuesNotInclusion returns distinct numeric values for the
// given name where the value is not contained in the provided set of values,
// considering only entries at or before targetBlock.
func (s *PebbleStore) GetMatchingNumericValuesNotInclusion(ctx context.Context, reader pebble.Reader, name string, values []uint64, targetBlock uint64) ([]uint64, error) {
	set := make(map[uint64]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return s.scanDistinctNumericValues(reader, name, targetBlock, func(v uint64) bool {
		_, ok := set[v]
		return !ok
	})
}
