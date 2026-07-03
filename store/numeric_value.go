package store

// NumericValueToSQL converts a numeric attribute value (uint64) to the
// form stored in SQLite.
//
// Two problems are solved at once:
//
//  1. SQLite integers are signed 64-bit, and Go's database/sql layer
//     refuses to bind a uint64 with the high bit set (values of 2^63 or
//     more). Before this conversion existed, indexing an entity with such
//     a numeric attribute failed with
//
//	sql: converting argument $2 type: uint64 values with high bit set are not supported
//
//     which permanently stopped the event follower and with it all
//     arkiv_query serving.
//
//  2. Range queries (<, <=, >, >=) compare the stored column with SQL's
//     signed ordering, which must therefore agree with unsigned numeric
//     ordering across the full uint64 range.
//
// Flipping the top bit and reinterpreting the result as int64 solves
// both: the mapping is one-to-one (equality and IN stay exact) and
// strictly increasing — 0 maps to the most negative int64, 2^64-1 to the
// most positive, so signed SQL order equals numeric order everywhere.
//
// Existing databases written before this encoding hold the raw value in
// the column; migration 000002 re-encodes them once on startup.
func NumericValueToSQL(v uint64) int64 {
	return int64(v ^ (1 << 63))
}

// NumericValuesToSQL converts a slice of numeric attribute values with
// NumericValueToSQL.
func NumericValuesToSQL(vs []uint64) []int64 {
	out := make([]int64, len(vs))
	for i, v := range vs {
		out[i] = NumericValueToSQL(v)
	}
	return out
}
