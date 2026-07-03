package store

import (
	"database/sql/driver"
	"fmt"
)

// numericValueBias is XORed into a numeric attribute value before it is
// bound to SQL and after it is read back.
const numericValueBias = uint64(1) << 63

// NumericValue is a uint64 numeric attribute value as stored in SQLite.
//
// SQLite integers are signed 64-bit and Go's database/sql refuses to bind
// a uint64 with the high bit set (values of 2^63 or more), which used to
// make such values fatal to the event follower:
//
//	sql: converting argument $2 type: uint64 values with high bit set are not supported
//
// The value is stored with its top bit flipped (equivalent to subtracting
// 2^63), mapping [0, 2^64) one-to-one onto [-2^63, 2^63) while preserving
// order. Unsigned ordering therefore matches SQLite's signed INTEGER
// ordering, so range comparisons (<, <=, >, >=) stay correct across the
// full uint64 range. Existing rows written with the previous raw encoding
// are converted by migration 000002.
type NumericValue uint64

// NumericValues converts a slice of raw uint64 values to NumericValue.
func NumericValues(vs []uint64) []NumericValue {
	out := make([]NumericValue, len(vs))
	for i, v := range vs {
		out[i] = NumericValue(v)
	}
	return out
}

// Valuer interface for writing to DB
func (v NumericValue) Value() (driver.Value, error) {
	return int64(uint64(v) ^ numericValueBias), nil
}

// Scanner interface for reading from DB
func (v *NumericValue) Scan(src any) error {
	i, ok := src.(int64)
	if !ok {
		return fmt.Errorf("expected int64, got %T", src)
	}
	*v = NumericValue(uint64(i) ^ numericValueBias)
	return nil
}
