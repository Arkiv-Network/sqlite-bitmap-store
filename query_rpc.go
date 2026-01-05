package sqlitebitmapstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/query"
	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
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
	ResultsPerPage *uint64      `json:"resultsPerPage"`
	Cursor         string       `json:"cursor"`
}

func (o *Options) GetAtBlock() uint64 {

	if o == nil {
		return 0
	}

	if o.AtBlock != nil {
		return *o.AtBlock
	}
	return 0
}

func (o *Options) GetResultsPerPage() uint64 {
	if o == nil {
		return 200
	}

	if o.ResultsPerPage == nil {
		return 200
	}

	return *o.ResultsPerPage
}

func (o *Options) GetIncludeData() IncludeData {
	if o == nil || o.IncludeData == nil {
		return IncludeData{
			Key:         true,
			ContentType: true,
			Payload:     true,
		}
	}
	return *o.IncludeData
}

type QueryResponse struct {
	Data        []json.RawMessage `json:"data"`
	BlockNumber hexutil.Uint64    `json:"blockNumber"`
	Cursor      *string           `json:"cursor,omitempty"`
	TotalCount  hexutil.Uint64    `json:"totalCount"`
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

	StringAttributes  []StringAttribute  `json:"stringAttributes,omitempty"`
	NumericAttributes []NumericAttribute `json:"numericAttributes,omitempty"`
}

type StringAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type NumericAttribute struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

const maxResultBytes = 512 * 1024 * 1024

func (s *SQLiteStore) QueryEntities(
	ctx context.Context,
	queryStr string,
	options *Options,
) (*QueryResponse, error) {

	// TODO: wait for the block height

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

	res.TotalCount = hexutil.Uint64(bitmap.GetCardinality())

	it := bitmap.ReverseIterator()

	maxResults := options.GetResultsPerPage()

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

		payloads, err := queries.RetrievePayloads(ctx, nextIDs)
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
		res.Cursor = pointerOf(fmt.Sprintf("0x%x", *lastID))
	}

	return res, nil

}
func pointerOf[T any](v T) *T {
	return &v
}

func toPayload(r store.RetrievePayloadsRow, includeData IncludeData) *EntityData {
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

	// TODO fix splitting of synthetic from organic attributes
	if includeData.Attributes || includeData.SyntheticAttributes {

		res.StringAttributes = make([]StringAttribute, 0)
		for k, v := range r.StringAttributes.Values {
			res.StringAttributes = append(res.StringAttributes, StringAttribute{Key: k, Value: v})
		}

		res.NumericAttributes = make([]NumericAttribute, 0)
		for k, v := range r.NumericAttributes.Values {
			res.NumericAttributes = append(res.NumericAttributes, NumericAttribute{Key: k, Value: v})
		}
	}

	return res

}
