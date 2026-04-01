package query

import (
	"testing"
)

// FuzzParse exercises the query DSL parser and normalizer with arbitrary input.
// The goal is to ensure Parse never panics regardless of what a caller sends.
//
// Notable panic sites in normalize.go that the fuzzer can reach:
//   - Expression.invert(): "This should never happen!" (via NOT-paren paths)
//   - EqualExpr.Normalize(): "Called EqualExpr::Normalize on a paren, this is a bug!"
//   - EqualExpr.Normalize(): "This should not happen!"
//   - EqualExpr.invert():    "This should not happen!"
func FuzzParse(f *testing.F) {
	seeds := []string{
		// match-all shortcuts
		"$all",
		"*",

		// equality
		`name = "value"`,
		`name != "value"`,
		`$key = 0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890`,
		`$owner = 0x1234567890abcdef1234567890abcdef12345678`,
		`$creator = 0x1234567890abcdef1234567890abcdef12345678`,
		`$expiration = 1000`,
		`$sequence = 42`,

		// comparisons (numeric and string)
		`count > 5`,
		`count >= 5`,
		`count < 5`,
		`count <= 5`,
		`name > "bar"`,
		`name >= "bar"`,

		// glob
		`name ~ "foo*"`,
		`name !~ "foo*"`,
		`name glob "foo*"`,
		`name not glob "foo*"`,

		// inclusion
		`name IN ("a" "b" "c")`,
		`name NOT IN ("a" "b")`,
		`count IN (1 2 3)`,
		`count NOT IN (1 2 3)`,

		// boolean combinations
		`name = "x" || other = "y"`,
		`name = "x" && other = "y"`,
		`name = "x" OR other = "y"`,
		`name = "x" AND other = "y"`,
		`name = "x" or other = "y"`,
		`name = "x" and other = "y"`,

		// parenthesised groups
		`(name = "x")`,
		`(name = "x" && other = "y")`,
		`(name = "x" || other = "y") && third = "z"`,
		`name = "a" && (b = "c" || d = "e")`,

		// NOT-paren (exercises the invert() path in normalize.go)
		`!(name = "value")`,
		`not(name = "value")`,
		`NOT(name = "value")`,
		`!(name = "x" && other = "y")`,
		`!(name = "x" || other = "y")`,
		`!(!(name = "value"))`,
		`!(name ~ "foo*")`,
		`!(name IN ("a" "b"))`,
		`!(count > 5)`,

		// deeply nested
		`((name = "x"))`,
		`!((name = "x" || y = "z") && w = "v")`,
		`(a = "1" || b = "2") && (c = "3" || d = "4")`,

		// empty / near-empty
		"",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Must not panic. Errors are fine — they represent rejected queries.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Parse panicked on input %q: %v", input, r)
			}
		}()
		_, _ = Parse(input)
	})
}
