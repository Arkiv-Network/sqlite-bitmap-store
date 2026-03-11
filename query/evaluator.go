package query

import (
	"context"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

// Evaluator abstracts the storage-layer operations needed by the query
// evaluation logic. It uses flat parameters instead of sqlc-generated param
// structs, decoupling the evaluator from any specific storage backend
// (SQLite, PebbleDB, etc.).
type Evaluator interface {
	// EvaluateAllCurrent returns the IDs of all currently active entities.
	EvaluateAllCurrent(ctx context.Context) ([]uint64, error)

	// EvaluateAllAtBlock returns the IDs of all entities active at the given block.
	EvaluateAllAtBlock(ctx context.Context, block uint64) ([]uint64, error)

	// --- String value enumeration ---

	GetMatchingStringValuesEqual(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesNotEqual(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesLessThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesGreaterThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesLessOrEqualThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesGreaterOrEqualThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesGlob(ctx context.Context, name, pattern string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesNotGlob(ctx context.Context, name, pattern string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesInclusion(ctx context.Context, name string, values []string, targetBlock uint64) ([]string, error)
	GetMatchingStringValuesNotInclusion(ctx context.Context, name string, values []string, targetBlock uint64) ([]string, error)

	// --- Numeric value enumeration ---

	GetMatchingNumericValuesEqual(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesNotEqual(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesLessThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesGreaterThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesLessOrEqualThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesGreaterOrEqualThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesInclusion(ctx context.Context, name string, values []uint64, targetBlock uint64) ([]uint64, error)
	GetMatchingNumericValuesNotInclusion(ctx context.Context, name string, values []uint64, targetBlock uint64) ([]uint64, error)

	// --- Bitmap reconstruction ---

	ReconstructStringBitmapAtBlock(ctx context.Context, name, value string, block uint64) (*store.Bitmap, error)
	ReconstructNumericBitmapAtBlock(ctx context.Context, name string, value, block uint64) (*store.Bitmap, error)
}
