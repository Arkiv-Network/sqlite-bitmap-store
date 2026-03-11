package pebblestore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/Arkiv-Network/sqlite-bitmap-store/query"
	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

const QueryResultCountLimit uint64 = 200

type IncludeData struct {
	Key                         bool `json:"key"`
	Attributes                  bool `json:"attributes"`
	SyntheticAttributes         bool `json:"syntheticAttributes"`
	Payload                     bool `json:"payload"`
	ContentType                 bool `json:"contentType"`
	Expiration                  bool `json:"expiration"`
	Creator                     bool `json:"creator"`
	Owner                       bool `json:"owner"`
	CreatedAtBlock              bool `json:"createdAtBlock"`
	LastModifiedAtBlock         bool `json:"lastModifiedAtBlock"`
	TransactionIndexInBlock     bool `json:"transactionIndexInBlock"`
	OperationIndexInTransaction bool `json:"operationIndexInTransaction"`
}

type Options struct {
	AtBlock        *hexutil.Uint64 `json:"atBlock,omitempty"`
	IncludeData    *IncludeData    `json:"includeData,omitempty"`
	ResultsPerPage *hexutil.Uint64 `json:"resultsPerPage,omitempty"`
	Cursor         string          `json:"cursor,omitempty"`
}

func (o *Options) GetAtBlock() uint64 {
	if o == nil || o.AtBlock == nil {
		return 0
	}
	return uint64(*o.AtBlock)
}

func (o *Options) GetResultsPerPage() uint64 {
	if o == nil || o.ResultsPerPage == nil || uint64(*o.ResultsPerPage) > QueryResultCountLimit {
		return QueryResultCountLimit
	}
	return uint64(*o.ResultsPerPage)
}

func (o *Options) GetIncludeData() IncludeData {
	if o == nil || o.IncludeData == nil {
		return IncludeData{
			Key:         true,
			ContentType: true,
			Payload:     true,
			Creator:     true,
			Owner:       true,
			Attributes:  true,
			Expiration:  true,
		}
	}
	return *o.IncludeData
}

func (o *Options) GetCursor() (*uint64, error) {
	if o == nil || o.Cursor == "" {
		return nil, nil
	}

	cursor, err := hexutil.DecodeUint64(o.Cursor)
	if err != nil {
		return nil, fmt.Errorf("error decoding cursor: %w", err)
	}

	return &cursor, nil
}

type QueryResponse struct {
	Data        []json.RawMessage `json:"data"`
	BlockNumber hexutil.Uint64    `json:"blockNumber"`
	Cursor      *string           `json:"cursor,omitempty"`
}

type EntityData struct {
	Key                         *common.Hash    `json:"key,omitempty"`
	Value                       hexutil.Bytes   `json:"value,omitempty"`
	ContentType                 *string         `json:"contentType,omitempty"`
	ExpiresAt                   *uint64         `json:"expiresAt,omitempty"`
	Creator                     *common.Address `json:"creator,omitempty"`
	Owner                       *common.Address `json:"owner,omitempty"`
	CreatedAtBlock              *uint64         `json:"createdAtBlock,omitempty"`
	LastModifiedAtBlock         *uint64         `json:"lastModifiedAtBlock,omitempty"`
	TransactionIndexInBlock     *uint64         `json:"transactionIndexInBlock,omitempty"`
	OperationIndexInTransaction *uint64         `json:"operationIndexInTransaction,omitempty"`

	StringAttributes  []Attribute[string] `json:"stringAttributes,omitempty"`
	NumericAttributes []Attribute[uint64] `json:"numericAttributes,omitempty"`
}

type Attribute[T any] struct {
	Key   string `json:"key"`
	Value T      `json:"value"`
}

// snapshotEvaluator wraps a PebbleStore and a pebble.Snapshot to implement
// the query.Evaluator interface. Every method delegates to the corresponding
// PebbleStore method, passing the snapshot as the pebble.Reader.
type snapshotEvaluator struct {
	store *PebbleStore
	snap  *pebble.Snapshot
}

// Compile-time check that snapshotEvaluator satisfies query.Evaluator.
var _ query.Evaluator = (*snapshotEvaluator)(nil)

func (e *snapshotEvaluator) EvaluateAllCurrent(ctx context.Context) ([]uint64, error) {
	return e.store.EvaluateAllCurrent(ctx, e.snap)
}

func (e *snapshotEvaluator) EvaluateAllAtBlock(ctx context.Context, block uint64) ([]uint64, error) {
	return e.store.EvaluateAllAtBlock(ctx, e.snap, block)
}

// --- String value enumeration ---

func (e *snapshotEvaluator) GetMatchingStringValuesEqual(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesEqual(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesNotEqual(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesNotEqual(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesLessThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesLessThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesGreaterThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesGreaterThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesLessOrEqualThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesLessOrEqualThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesGreaterOrEqualThan(ctx context.Context, name, value string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesGreaterOrEqualThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesGlob(ctx context.Context, name, pattern string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesGlob(ctx, e.snap, name, pattern, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesNotGlob(ctx context.Context, name, pattern string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesNotGlob(ctx, e.snap, name, pattern, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesInclusion(ctx context.Context, name string, values []string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesInclusion(ctx, e.snap, name, values, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingStringValuesNotInclusion(ctx context.Context, name string, values []string, targetBlock uint64) ([]string, error) {
	return e.store.GetMatchingStringValuesNotInclusion(ctx, e.snap, name, values, targetBlock)
}

// --- Numeric value enumeration ---

func (e *snapshotEvaluator) GetMatchingNumericValuesEqual(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesEqual(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesNotEqual(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesNotEqual(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesLessThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesLessThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesGreaterThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesGreaterThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesLessOrEqualThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesLessOrEqualThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesGreaterOrEqualThan(ctx context.Context, name string, value, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesGreaterOrEqualThan(ctx, e.snap, name, value, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesInclusion(ctx context.Context, name string, values []uint64, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesInclusion(ctx, e.snap, name, values, targetBlock)
}

func (e *snapshotEvaluator) GetMatchingNumericValuesNotInclusion(ctx context.Context, name string, values []uint64, targetBlock uint64) ([]uint64, error) {
	return e.store.GetMatchingNumericValuesNotInclusion(ctx, e.snap, name, values, targetBlock)
}

// --- Bitmap reconstruction ---

func (e *snapshotEvaluator) ReconstructStringBitmapAtBlock(ctx context.Context, name, value string, block uint64) (*store.Bitmap, error) {
	return e.store.ReconstructStringBitmapAtBlock(ctx, e.snap, name, value, block)
}

func (e *snapshotEvaluator) ReconstructNumericBitmapAtBlock(ctx context.Context, name string, value, block uint64) (*store.Bitmap, error) {
	return e.store.ReconstructNumericBitmapAtBlock(ctx, e.snap, name, value, block)
}

const maxResultBytes = 512 * 1024 * 1024

// QueryEntities parses and evaluates a query string against the store,
// returning paginated entity data. A PebbleDB snapshot is taken at the
// beginning so that all reads within the query observe a consistent state.
func (s *PebbleStore) QueryEntities(
	ctx context.Context,
	queryStr string,
	options *Options,
) (*QueryResponse, error) {

	snap := s.db.NewSnapshot()
	defer snap.Close()

	// Wait for the block height to reach the requested atBlock, polling
	// with a 3-second timeout.
	var lastBlock uint64
	{
		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		for {
			var err error
			lastBlock, err = s.GetLastBlock(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting last block: %w", err)
			}
			if lastBlock >= options.GetAtBlock() {
				break
			}
			select {
			case <-timeoutCtx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		cancel()
	}

	// The effective query block: the requested atBlock, or the latest block.
	queryBlock := options.GetAtBlock()
	if queryBlock == 0 {
		queryBlock = lastBlock
	}

	q, err := query.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %w", err)
	}

	eval := &snapshotEvaluator{store: s, snap: snap}

	bitmap, err := q.Evaluate(ctx, eval, options.GetAtBlock())
	if err != nil {
		return nil, fmt.Errorf("error evaluating query: %w", err)
	}

	cursor, err := options.GetCursor()
	if err != nil {
		return nil, fmt.Errorf("error decoding cursor: %w", err)
	}

	// The cursor contains the last value that was included in the previous page.
	// We create a bitmask by creating an empty bitmap, and then flipping the bits
	// from 0 to (cursor - 1) to 1, so that we only include values below the cursor
	// value.
	if cursor != nil {
		s.log.Info("decoded cursor", "value", *cursor)
		cursorMask := roaring64.New()
		cursorMask.AddRange(0, *cursor)
		bitmap.And(cursorMask)
	}

	it := bitmap.ReverseIterator()

	maxResults := options.GetResultsPerPage()

	res := &QueryResponse{
		Data:        []json.RawMessage{},
		BlockNumber: hexutil.Uint64(queryBlock),
		Cursor:      nil,
	}

	nextIDs := func(max uint64) []uint64 {
		ids := []uint64{}
		for range max {
			if !it.HasNext() {
				break
			}
			ids = append(ids, it.Next())
		}
		return ids
	}

	totalBytes := uint64(0)
	finished := true
	var lastID *uint64

fillLoop:
	for it.HasNext() {

		nextBatchSize := min(maxResults-uint64(len(res.Data)), 10)

		nextIDs := nextIDs(nextBatchSize)

		payloads, err := s.RetrievePayloads(snap, nextIDs)
		if err != nil {
			return nil, fmt.Errorf("error retrieving payloads: %w", err)
		}

		for _, payload := range payloads {

			lastID = &payload.ID

			ed := toPayload(payload, options.GetIncludeData())
			d, err := json.Marshal(ed)
			if err != nil {
				return nil, fmt.Errorf("error marshalling entity data: %w", err)
			}
			res.Data = append(res.Data, d)
			totalBytes += uint64(len(d))

			if totalBytes > maxResultBytes {
				finished = false
				break fillLoop
			}

			if uint64(len(res.Data)) >= maxResults {
				finished = false
				break fillLoop
			}

		}

	}

	if !finished {
		res.Cursor = pointerOf(hexutil.EncodeUint64(*lastID))
	}

	return res, nil
}

func pointerOf[T any](v T) *T {
	return &v
}

func filterAttributes[T any](predicate func(string) bool, m map[string]T) []Attribute[T] {
	res := []Attribute[T]{}

	for k, v := range m {
		if !predicate(k) {
			continue
		}
		res = append(res, Attribute[T]{Key: k, Value: v})
	}

	slices.SortFunc(res, func(i, j Attribute[T]) int {
		return strings.Compare(i.Key, j.Key)
	})

	return res
}

func syntheticPredicate(k string) bool {
	return strings.HasPrefix(k, "$")
}

func nonSyntheticPredicate(k string) bool {
	return !strings.HasPrefix(k, "$")
}

func anyPredicate(string) bool {
	return true
}

func toPayload(r RetrievePayloadsRow, includeData IncludeData) *EntityData {
	res := &EntityData{}
	if includeData.Key {
		res.Key = pointerOf(common.BytesToHash(r.EntityKey))
	}
	if includeData.Payload {
		res.Value = r.Payload
	}

	if includeData.ContentType {
		res.ContentType = &r.ContentType
	}

	switch {
	case includeData.Attributes && includeData.SyntheticAttributes:
		res.StringAttributes = filterAttributes(anyPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(anyPredicate, r.NumericAttributes.Values)
	case includeData.Attributes:
		res.StringAttributes = filterAttributes(nonSyntheticPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(nonSyntheticPredicate, r.NumericAttributes.Values)
	case includeData.SyntheticAttributes:
		res.StringAttributes = filterAttributes(syntheticPredicate, r.StringAttributes.Values)
		res.NumericAttributes = filterAttributes(syntheticPredicate, r.NumericAttributes.Values)
	}

	if includeData.Expiration {
		res.ExpiresAt = pointerOf(r.NumericAttributes.Values["$expiration"])
	}

	if includeData.Creator {
		res.Creator = pointerOf(common.HexToAddress(r.StringAttributes.Values["$creator"]))
	}

	if includeData.Owner {
		res.Owner = pointerOf(common.HexToAddress(r.StringAttributes.Values["$owner"]))
	}

	if includeData.CreatedAtBlock {
		res.CreatedAtBlock = pointerOf(r.NumericAttributes.Values["$createdAtBlock"])
	}

	if includeData.LastModifiedAtBlock {
		res.LastModifiedAtBlock = pointerOf(r.NumericAttributes.Values["$lastModifiedAtBlock"])
	}

	if includeData.TransactionIndexInBlock {
		res.TransactionIndexInBlock = pointerOf(r.NumericAttributes.Values["$txIndex"])
	}

	if includeData.OperationIndexInTransaction {
		res.OperationIndexInTransaction = pointerOf(r.NumericAttributes.Values["$opIndex"])
	}

	return res
}
