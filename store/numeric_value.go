package store

// NumericValueToSQL converts a numeric attribute value (uint64) to the
// form stored in SQLite.
//
// SQLite integers are signed 64-bit, and Go's database/sql layer refuses
// to bind a uint64 with the high bit set (values of 2^63 or more). Before
// this conversion existed, indexing an entity with such a numeric
// attribute failed with:
//
//	sql: converting argument $2 type: uint64 values with high bit set are not supported
//
// which permanently stopped the event follower and with it all
// arkiv_query serving.
//
// We store the value's raw 8 bytes reinterpreted as an int64 (two's
// complement). The mapping is one-to-one, so equality and IN lookups stay
// exact for every possible value, and values below 2^63 — which is all
// realistic data — are stored as the same number as before, keeping
// existing databases valid with no migration.
//
// Known trade-off: values of 2^63 or more become negative in SQL order,
// so range comparisons (<, <=, >, >=) place them below small values
// instead of above. Correcting the ordering for that extreme range needs
// an order-preserving re-encoding plus a data migration of existing rows;
// that is intentionally left as a follow-up.
func NumericValueToSQL(v uint64) int64 {
	return int64(v)
}

// NumericValuesToSQL converts a slice of numeric attribute values with
// NumericValueToSQL.
func NumericValuesToSQL(vs []uint64) []int64 {
	out := make([]int64, len(vs))
	for i, v := range vs {
		out[i] = int64(v)
	}
	return out
}
