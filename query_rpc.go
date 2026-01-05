package sqlitebitmapstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/query"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type IncludeData struct {
	Key                         bool `json:"key"`
	Attributes                  bool `json:"attributes"`
	SyntheticAttributes         bool `json:"syntheticAttributes"`
	Payload                     bool `json:"payload"`
	ContentType                 bool `json:"contentType"`
	Expiration                  bool `json:"expiration"`
	Owner                       bool `json:"owner"`
	CreatedAtBlock              bool `json:"createdAtBlock"`
	LastModifiedAtBlock         bool `json:"lastModifiedAtBlock"`
	TransactionIndexInBlock     bool `json:"transactionIndexInBlock"`
	OperationIndexInTransaction bool `json:"operationIndexInTransaction"`
}

type Options struct {
	AtBlock        *uint64      `json:"atBlock"`
	IncludeData    *IncludeData `json:"includeData"`
	ResultsPerPage uint64       `json:"resultsPerPage"`
	Cursor         string       `json:"cursor"`
}

func (o Options) GetAtBlock() uint64 {
	if o.AtBlock != nil {
		return *o.AtBlock
	}
	return 0

}

type QueryResponse struct {
	Data        []json.RawMessage `json:"data"`
	BlockNumber uint64            `json:"blockNumber"`
	Cursor      *string           `json:"cursor,omitempty"`
}

type EntityData struct {
	Key                         *common.Hash    `json:"key,omitempty"`
	Value                       hexutil.Bytes   `json:"value,omitempty"`
	ContentType                 *string         `json:"contentType,omitempty"`
	ExpiresAt                   *uint64         `json:"expiresAt,omitempty"`
	Owner                       *common.Address `json:"owner,omitempty"`
	CreatedAtBlock              *uint64         `json:"createdAtBlock,omitempty"`
	LastModifiedAtBlock         *uint64         `json:"lastModifiedAtBlock,omitempty"`
	TransactionIndexInBlock     *uint64         `json:"transactionIndexInBlock,omitempty"`
	OperationIndexInTransaction *uint64         `json:"operationIndexInTransaction,omitempty"`

	StringAttributes  []StringAnnotation  `json:"stringAttributes,omitempty"`
	NumericAttributes []NumericAnnotation `json:"numericAttributes,omitempty"`
}

type StringAnnotation struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type NumericAnnotation struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

const maxResultBytes = 512 * 1024 * 1024

func (s *SQLiteStore) QueryEntities(
	ctx context.Context,
	queryStr string,
	options *Options,
) (*QueryResponse, error) {

	// TOOD: wait for the block height

	res := &QueryResponse{
		Data:        []json.RawMessage{},
		BlockNumber: 0,
		Cursor:      nil,
	}

	q, err := query.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %w", err)
	}

	queries := s.NewQueries()

	bitmap, err := q.Evaluate(
		ctx,
		queries,
	)

	if err != nil {
		return nil, fmt.Errorf("error evaluating query: %w", err)
	}

	it := bitmap.ReverseIterator()

	maxResults := options.ResultsPerPage

	if maxResults == 0 {
		maxResults = 200
	}

	nextIDs := func(max uint64) []uint64 {
		ids := []uint64{}
		for i := uint64(0); i < max; i++ {
			if !it.HasNext() {
				break
			}
		}
		return ids
	}

	// totalBytes := uint64(0)

	for it.HasNext() {

		nextBatchSize := min(maxResults-uint64(len(res.Data)), 10)

		nextIDs := nextIDs(nextBatchSize)

		_, err := queries.RetrievePayloads(ctx, nextIDs)
		if err != nil {
			return nil, fmt.Errorf("error retrieving payloads: %w", err)
		}

		// for _, payload := range payloads {

		// 	// ed :=

		// }

	}

	return res, nil

}

// func toPayload(r RetrievePayloadsRow, includeData *IncludeData) *query.EntityData {
// 	return &query.EntityData{
// 		Key:                         r.EntityKey,
// 		Value:                       r.Payload,
// 		ContentType:                 r.ContentType,
// 		ExpiresAt:                   r.ExpiresAt,
// 		Owner:                       r.Owner,
// 		CreatedAtBlock:              r.CreatedAtBlock,
// 		LastModifiedAtBlock:         r.LastModifiedAtBlock,
// 		TransactionIndexInBlock:     r.TransactionIndexInBlock,
// 		OperationIndexInTransaction: r.OperationIndexInTransaction,
// 	}
// }
