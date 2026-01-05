package query

import (
	"cmp"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

const QueryResultCountLimit uint64 = 200

// ResponseSize is 256 bytes for the overhead of the 'envelope' around the entity data
// and the separator characters in between
const ResponseSize int = 256

// MaxResponseSize is 512 kb
const MaxResponseSize int = 512 * 1024 * 1024

type Column struct {
	Name          string
	QualifiedName string
	// If this is a byte column, we need to decode it when we get it from the json-encoded cursor
	IsBytes bool
}

func (c Column) selector() string {
	return fmt.Sprintf("%s AS %s", c.QualifiedName, c.Name)
}

func (c Column) Compare(other Column) int {
	return cmp.Compare(c.Name, other.Name)
}

type QueryOptions struct {
	AtBlock     uint64
	IncludeData *IncludeData
	Columns     []Column
	Cursor      []CursorValue

	// Cache the sorted list of unique columns to fetch
	allColumnsSorted []string

	Log *slog.Logger
}

func NewQueryOptions(log *slog.Logger, latestHead uint64, options *InternalQueryOptions) (*QueryOptions, error) {
	queryOptions := QueryOptions{
		Log:         log,
		IncludeData: options.IncludeData,
	}

	queryOptions.Columns = []Column{}

	// We always need the primary key of the payloads table because of sorting
	queryOptions.Columns = append(queryOptions.Columns, Column{
		Name:          "from_block",
		QualifiedName: "e.from_block",
	})
	queryOptions.Columns = append(queryOptions.Columns, Column{
		Name:          "entity_key",
		QualifiedName: "e.entity_key",
		IsBytes:       true,
	})

	if options.IncludeData.Payload {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "payload",
			QualifiedName: "e.payload",
		})
	}
	if options.IncludeData.ContentType {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "content_type",
			QualifiedName: "e.content_type",
		})
	}
	if options.IncludeData.Attributes {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "string_attributes",
			QualifiedName: "e.string_attributes",
		})
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "numeric_attributes",
			QualifiedName: "e.numeric_attributes",
		})
	}

	if options.IncludeData.Owner {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "owner",
			QualifiedName: "ownerAttrs.Value",
		})
	}
	if options.IncludeData.Expiration {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "expires_at",
			QualifiedName: "expirationAttrs.Value",
		})
	}
	if options.IncludeData.CreatedAtBlock {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "created_at_block",
			QualifiedName: "createdAtBlockAttrs.Value",
		})
	}
	if options.IncludeData.LastModifiedAtBlock ||
		options.IncludeData.TransactionIndexInBlock ||
		options.IncludeData.OperationIndexInTransaction {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:          "sequence",
			QualifiedName: "sequenceAttrs.Value",
		})
	}

	// Sort so that we can use binary search later
	slices.SortFunc(queryOptions.Columns, Column.Compare)

	queryOptions.AtBlock = latestHead

	if options.AtBlock != nil {
		queryOptions.AtBlock = *options.AtBlock
	}

	return &queryOptions, nil
}

func (opts *QueryOptions) GetColumnIndex(column string) (int, error) {
	ix, found := slices.BinarySearchFunc(opts.Columns, column, func(a Column, b string) int {
		return cmp.Compare(a.Name, b)
	})

	if !found {
		return -1, fmt.Errorf("unknown column %s", column)
	}
	return ix, nil
}

func (opts *QueryOptions) columnString() string {
	if len(opts.Columns) == 0 {
		return "1"
	}

	columns := make([]string, 0, len(opts.Columns))
	for _, c := range opts.Columns {
		columns = append(columns, c.selector())
	}

	return strings.Join(columns, ", ")
}
