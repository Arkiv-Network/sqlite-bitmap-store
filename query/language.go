package query

import (
	"context"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

const AnnotationIdentRegex string = `[\p{L}_][\p{L}\p{N}_]*`

// Define the lexer with distinct tokens for each operator and parentheses.
var lex = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Whitespace", Pattern: `[ \t\n\r]+`},
	{Name: "LParen", Pattern: `\(`},
	{Name: "RParen", Pattern: `\)`},
	{Name: "And", Pattern: `&&`},
	{Name: "Or", Pattern: `\|\|`},
	{Name: "Neq", Pattern: `!=`},
	{Name: "Eq", Pattern: `=`},
	{Name: "Geqt", Pattern: `>=`},
	{Name: "Leqt", Pattern: `<=`},
	{Name: "Gt", Pattern: `>`},
	{Name: "Lt", Pattern: `<`},
	{Name: "NotGlob", Pattern: `!~`},
	{Name: "Glob", Pattern: `~`},
	{Name: "Not", Pattern: `!`},
	{Name: "EntityKey", Pattern: `0x[a-fA-F0-9]{64}`},
	{Name: "Address", Pattern: `0x[a-fA-F0-9]{40}`},
	{Name: "String", Pattern: `"(?:[^"\\]|\\.)*"`},
	{Name: "Number", Pattern: `[0-9]+`},
	{Name: "Ident", Pattern: AnnotationIdentRegex},
	// Meta-annotations, should start with $
	{Name: "Owner", Pattern: `\$owner`},
	{Name: "Creator", Pattern: `\$creator`},
	{Name: "Key", Pattern: `\$key`},
	{Name: "Expiration", Pattern: `\$expiration`},
	{Name: "Sequence", Pattern: `\$sequence`},
	{Name: "All", Pattern: `\$all`},
	{Name: "Star", Pattern: `\*`},
})

type TopLevel struct {
	Expression *Expression `parser:"@@ | All | Star"`
}

func (t *TopLevel) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return t.Expression.Evaluate(ctx, q)
}

// Expression is the top-level rule.
type Expression struct {
	Or OrExpression `parser:"@@"`
}

func (e *Expression) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return e.Or.Evaluate(ctx, q)
}

// OrExpression handles expressions connected with ||.
type OrExpression struct {
	Left  AndExpression `parser:"@@"`
	Right []*OrRHS      `parser:"@@*"`
}

func (e *OrExpression) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	lhs, err := e.Left.Evaluate(ctx, q)
	if err != nil {
		return nil, err
	}

	for _, rhs := range e.Right {
		rhs, err := rhs.Evaluate(ctx, q)
		if err != nil {
			return nil, err
		}
		lhs.Or(rhs)
	}

	return lhs, nil
}

// OrRHS represents the right-hand side of an OR.
type OrRHS struct {
	Expr AndExpression `parser:"(Or | 'OR' | 'or') @@"`
}

func (e *OrRHS) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return e.Expr.Evaluate(ctx, q)
}

// AndExpression handles expressions connected with &&.
type AndExpression struct {
	Left  EqualExpr `parser:"@@"`
	Right []*AndRHS `parser:"@@*"`
}

func (e *AndExpression) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	lhs, err := e.Left.Evaluate(ctx, q)
	if err != nil {
		return nil, err
	}

	for _, rhs := range e.Right {
		rhs, err := rhs.Evaluate(ctx, q)
		if err != nil {
			return nil, err
		}
		lhs.And(rhs)
	}

	return lhs, nil
}

// AndRHS represents the right-hand side of an AND.
type AndRHS struct {
	Expr EqualExpr `parser:"(And | 'AND' | 'and') @@"`
}

func (e *AndRHS) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return e.Expr.Evaluate(ctx, q)
}

// EqualExpr can be either an equality or a parenthesized expression.
type EqualExpr struct {
	Paren     *Paren     `parser:"  @@"`
	Assign    *Equality  `parser:"| @@"`
	Inclusion *Inclusion `parser:"| @@"`

	LessThan           *LessThan           `parser:"| @@"`
	LessOrEqualThan    *LessOrEqualThan    `parser:"| @@"`
	GreaterThan        *GreaterThan        `parser:"| @@"`
	GreaterOrEqualThan *GreaterOrEqualThan `parser:"| @@"`
	Glob               *Glob               `parser:"| @@"`
}

func (e *EqualExpr) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	switch {
	case e.Paren != nil:
		return e.Paren.Evaluate(ctx, q)
	case e.Assign != nil:
		return e.Assign.Evaluate(ctx, q)
	case e.Inclusion != nil:
		return e.Inclusion.Evaluate(ctx, q)
	case e.LessThan != nil:
		return e.LessThan.Evaluate(ctx, q)
	case e.LessOrEqualThan != nil:
		return e.LessOrEqualThan.Evaluate(ctx, q)
	case e.GreaterThan != nil:
		return e.GreaterThan.Evaluate(ctx, q)
	case e.GreaterOrEqualThan != nil:
		return e.GreaterOrEqualThan.Evaluate(ctx, q)
	case e.Glob != nil:
		return e.Glob.Evaluate(ctx, q)
	default:
		return nil, fmt.Errorf("unknown equal expression: %v", e)
	}
}

type Paren struct {
	IsNot  bool       `parser:"@(Not | 'NOT' | 'not')?"`
	Nested Expression `parser:"LParen @@ RParen"`
}

func (e *Paren) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (*roaring64.Bitmap, error) {
	return e.Nested.Evaluate(ctx, q)
}

type Glob struct {
	Var   string `parser:"@Ident"`
	IsNot bool   `parser:"((Glob | @NotGlob) | (@('NOT' | 'not')? ('GLOB' | 'glob')))"`
	Value string `parser:"@String"`
}

func (e *Glob) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	bm := roaring64.New()

	var bitmaps []*store.Bitmap

	if e.IsNot {
		bitmaps, err = q.EvaluateStringAttributeValueNotGlob(ctx, store.EvaluateStringAttributeValueNotGlobParams{
			Name:  e.Var,
			Value: e.Value,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateStringAttributeValueGlob(ctx, store.EvaluateStringAttributeValueGlobParams{
			Name:  e.Var,
			Value: e.Value,
		})
		if err != nil {
			return nil, err
		}
	}

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

type LessThan struct {
	Var   string `parser:"@Ident Lt"`
	Value Value  `parser:"@@"`
}

func (e *LessThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueLowerThan(ctx, store.EvaluateStringAttributeValueLowerThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueLowerThan(ctx, store.EvaluateNumericAttributeValueLowerThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

type LessOrEqualThan struct {
	Var   string `parser:"@Ident Leqt"`
	Value Value  `parser:"@@"`
}

func (e *LessOrEqualThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueLessOrEqualThan(ctx, store.EvaluateStringAttributeValueLessOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}
	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueLessOrEqualThan(ctx, store.EvaluateNumericAttributeValueLessOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

type GreaterThan struct {
	Var   string `parser:"@Ident Gt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueGreaterThan(ctx, store.EvaluateStringAttributeValueGreaterThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}

	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

type GreaterOrEqualThan struct {
	Var   string `parser:"@Ident Geqt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterOrEqualThan) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	var bitmaps []*store.Bitmap

	if e.Value.String != nil {
		bitmaps, err = q.EvaluateStringAttributeValueGreaterOrEqualThan(ctx, store.EvaluateStringAttributeValueGreaterOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.String,
		})
		if err != nil {
			return nil, err
		}

	} else {
		bitmaps, err = q.EvaluateNumericAttributeValueGreaterOrEqualThan(ctx, store.EvaluateNumericAttributeValueGreaterOrEqualThanParams{
			Name:  e.Var,
			Value: *e.Value.Number,
		})
		if err != nil {
			return nil, err
		}
	}

	bm := roaring64.New()

	for _, bitmap := range bitmaps {
		bm.Or(bitmap.Bitmap)
	}

	return bm, nil
}

// Equality represents a simple equality (e.g. name = 123).
type Equality struct {
	Var   string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot bool   `parser:"(Eq | @Neq)"`
	Value Value  `parser:"@@"`
}

func (e *Equality) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	if e.Value.String != nil {

		if e.IsNot {

			var bitmaps []*store.Bitmap
			bitmaps, err = q.EvaluateStringAttributeValueNotEqual(ctx, store.EvaluateStringAttributeValueNotEqualParams{
				Name:  e.Var,
				Value: *e.Value.String,
			})
			if err != nil {
				return nil, err
			}

			bm := roaring64.New()

			for _, bitmap := range bitmaps {
				bm.Or(bitmap.Bitmap)
			}

			return bm, nil

		} else {
			bm, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
				Name:  e.Var,
				Value: *e.Value.String,
			})
			if err != nil {
				return nil, err
			}

			return bm.Bitmap, nil
		}
	} else {
		if e.IsNot {

			var bitmaps []*store.Bitmap
			bitmaps, err = q.EvaluateNumericAttributeValueNotEqual(ctx, store.EvaluateNumericAttributeValueNotEqualParams{
				Name:  e.Var,
				Value: *e.Value.Number,
			})
			if err != nil {
				return nil, err
			}

			bm := roaring64.New()
			for _, bitmap := range bitmaps {
				bm.Or(bitmap.Bitmap)
			}

			return bm, nil
		} else {
			bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
				Name:  e.Var,
				Value: *e.Value.Number,
			})
			if err != nil {
				return nil, err
			}

			return bitmap.Bitmap, nil
		}
	}

}

type Inclusion struct {
	Var    string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot  bool   `parser:"(@('NOT'|'not')? ('IN'|'in'))"`
	Values Values `parser:"@@"`
}

func (e *Inclusion) Evaluate(
	ctx context.Context,
	q *store.Queries,
) (_ *roaring64.Bitmap, err error) {

	if len(e.Values.Strings) != 0 {

		var bitmaps []*store.Bitmap

		if e.IsNot {
			bitmaps, err = q.EvaluateStringAttributeValueNotInclusion(ctx, store.EvaluateStringAttributeValueNotInclusionParams{
				Name:   e.Var,
				Values: e.Values.Strings,
			})
			if err != nil {
				return nil, err
			}
		} else {

			bitmaps, err = q.EvaluateStringAttributeValueInclusion(ctx, store.EvaluateStringAttributeValueInclusionParams{
				Name:   e.Var,
				Values: e.Values.Strings,
			})
			if err != nil {
				return nil, err
			}
		}
		bm := roaring64.New()
		for _, bitmap := range bitmaps {
			bm.Or(bitmap.Bitmap)
		}
		return bm, nil

	} else {
		var bitmaps []*store.Bitmap

		if e.IsNot {
			bitmaps, err = q.EvaluateNumericAttributeValueNotInclusion(ctx, store.EvaluateNumericAttributeValueNotInclusionParams{
				Name:   e.Var,
				Values: e.Values.Numbers,
			})
			if err != nil {
				return nil, err
			}
		} else {
			bitmaps, err = q.EvaluateNumericAttributeValueInclusion(ctx, store.EvaluateNumericAttributeValueInclusionParams{
				Name:   e.Var,
				Values: e.Values.Numbers,
			})
			if err != nil {
				return nil, err
			}
		}
		bm := roaring64.New()
		for _, bitmap := range bitmaps {
			bm.Or(bitmap.Bitmap)
		}
		return bm, nil
	}

}

// Value is a literal value (a number or a string).
type Value struct {
	String *string `parser:"  (@String | @EntityKey | @Address)"`
	Number *uint64 `parser:"| @Number"`
}

type Values struct {
	Strings []string `parser:"  '(' (@String | @EntityKey | @Address)+ ')'"`
	Numbers []uint64 `parser:"| '(' @Number+ ')'"`
}

var Parser = participle.MustBuild[TopLevel](
	participle.Lexer(lex),
	participle.Elide("Whitespace"),
	participle.Unquote("String"),
)

func Parse(s string) (*TopLevel, error) {

	v, err := Parser.ParseString("", s)
	if err != nil {
		return nil, err
	}

	return v.Normalize(), nil
}
