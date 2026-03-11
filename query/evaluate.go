package query

import (
	"context"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func (t *AST) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (*roaring64.Bitmap, error) {
	if t.Expr == nil {
		var ids []uint64
		var err error
		if block == 0 {
			ids, err = q.EvaluateAllCurrent(ctx)
		} else {
			ids, err = q.EvaluateAllAtBlock(ctx, block)
		}
		if err != nil {
			return nil, err
		}
		bm := roaring64.New()
		bm.AddMany(ids)
		return bm, nil
	}
	return t.Expr.Evaluate(ctx, q, block)
}

func (e *ASTExpr) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (*roaring64.Bitmap, error) {
	return e.Or.Evaluate(ctx, q, block)
}

func (e *ASTOr) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (*roaring64.Bitmap, error) {
	var tmp *roaring64.Bitmap = nil

	for _, term := range e.Terms {
		bm, err := term.Evaluate(ctx, q, block)
		if err != nil {
			return nil, err
		}
		if tmp == nil {
			tmp = bm
		} else {
			tmp.Or(bm)
		}
	}

	return tmp, nil
}

func (e *ASTAnd) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (*roaring64.Bitmap, error) {
	var tmp *roaring64.Bitmap = nil

	for _, term := range e.Terms {
		bm, err := term.Evaluate(ctx, q, block)
		if err != nil {
			return nil, err
		}
		if tmp == nil {
			tmp = bm
		} else {
			tmp.And(bm)
		}
	}

	return tmp, nil
}

func (e *ASTTerm) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (*roaring64.Bitmap, error) {
	switch {
	case e.Assign != nil:
		return e.Assign.Evaluate(ctx, q, block)
	case e.Inclusion != nil:
		return e.Inclusion.Evaluate(ctx, q, block)
	case e.LessThan != nil:
		return e.LessThan.Evaluate(ctx, q, block)
	case e.LessOrEqualThan != nil:
		return e.LessOrEqualThan.Evaluate(ctx, q, block)
	case e.GreaterThan != nil:
		return e.GreaterThan.Evaluate(ctx, q, block)
	case e.GreaterOrEqualThan != nil:
		return e.GreaterOrEqualThan.Evaluate(ctx, q, block)
	case e.Glob != nil:
		return e.Glob.Evaluate(ctx, q, block)
	default:
		return nil, fmt.Errorf("unknown equal expression: %v", e)
	}
}

// reconstructAndOR fetches bitmap chains for each matching value, reconstructs
// each bitmap at the target block, and ORs them together.
func reconstructAndOR(ctx context.Context, q Evaluator, block uint64, reconstructor func(value string) (*store.Bitmap, error), values []string) (*roaring64.Bitmap, error) {
	bm := roaring64.New()
	for _, val := range values {
		reconstructed, err := reconstructor(val)
		if err != nil {
			return nil, err
		}
		if reconstructed != nil && reconstructed.Bitmap != nil {
			bm.Or(reconstructed.Bitmap)
		}
	}
	return bm, nil
}

func reconstructAndORNumeric(ctx context.Context, q Evaluator, block uint64, name string, values []uint64) (*roaring64.Bitmap, error) {
	bm := roaring64.New()
	for _, val := range values {
		reconstructed, err := q.ReconstructNumericBitmapAtBlock(ctx, name, val, block)
		if err != nil {
			return nil, err
		}
		if reconstructed != nil && reconstructed.Bitmap != nil {
			bm.Or(reconstructed.Bitmap)
		}
	}
	return bm, nil
}

func (e *Glob) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	var values []string
	if e.IsNot {
		values, err = q.GetMatchingStringValuesNotGlob(ctx, e.Var, e.Value, eb)
	} else {
		values, err = q.GetMatchingStringValuesGlob(ctx, e.Var, e.Value, eb)
	}
	if err != nil {
		return nil, err
	}

	return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
		return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
	}, values)
}

func (e *LessThan) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := q.GetMatchingStringValuesLessThan(ctx, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
			return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
		}, values)
	}

	values, err := q.GetMatchingNumericValuesLessThan(ctx, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructAndORNumeric(ctx, q, block, e.Var, values)
}

func (e *LessOrEqualThan) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := q.GetMatchingStringValuesLessOrEqualThan(ctx, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
			return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
		}, values)
	}

	values, err := q.GetMatchingNumericValuesLessOrEqualThan(ctx, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructAndORNumeric(ctx, q, block, e.Var, values)
}

func (e *GreaterThan) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := q.GetMatchingStringValuesGreaterThan(ctx, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
			return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
		}, values)
	}

	values, err := q.GetMatchingNumericValuesGreaterThan(ctx, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructAndORNumeric(ctx, q, block, e.Var, values)
}

func (e *GreaterOrEqualThan) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		values, err := q.GetMatchingStringValuesGreaterOrEqualThan(ctx, e.Var, *e.Value.String, eb)
		if err != nil {
			return nil, err
		}
		return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
			return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
		}, values)
	}

	values, err := q.GetMatchingNumericValuesGreaterOrEqualThan(ctx, e.Var, *e.Value.Number, eb)
	if err != nil {
		return nil, err
	}
	return reconstructAndORNumeric(ctx, q, block, e.Var, values)
}

func (e *Equality) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if e.Value.String != nil {
		if e.IsNot {
			values, err := q.GetMatchingStringValuesNotEqual(ctx, e.Var, *e.Value.String, eb)
			if err != nil {
				return nil, err
			}
			return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
				return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
			}, values)
		}

		bm, err := q.ReconstructStringBitmapAtBlock(ctx, e.Var, *e.Value.String, block)
		if err != nil {
			return nil, err
		}
		return bm.Bitmap, nil
	}

	if e.IsNot {
		values, err := q.GetMatchingNumericValuesNotEqual(ctx, e.Var, *e.Value.Number, eb)
		if err != nil {
			return nil, err
		}
		return reconstructAndORNumeric(ctx, q, block, e.Var, values)
	}

	bm, err := q.ReconstructNumericBitmapAtBlock(ctx, e.Var, *e.Value.Number, block)
	if err != nil {
		return nil, err
	}
	return bm.Bitmap, nil
}

func (e *Inclusion) Evaluate(
	ctx context.Context,
	q Evaluator,
	block uint64,
) (_ *roaring64.Bitmap, err error) {
	eb := store.EffectiveBlock(block)

	if len(e.Values.Strings) != 0 {
		var values []string
		if e.IsNot {
			values, err = q.GetMatchingStringValuesNotInclusion(ctx, e.Var, e.Values.Strings, eb)
		} else {
			values, err = q.GetMatchingStringValuesInclusion(ctx, e.Var, e.Values.Strings, eb)
		}
		if err != nil {
			return nil, err
		}
		return reconstructAndOR(ctx, q, block, func(val string) (*store.Bitmap, error) {
			return q.ReconstructStringBitmapAtBlock(ctx, e.Var, val, block)
		}, values)
	}

	var values []uint64
	if e.IsNot {
		values, err = q.GetMatchingNumericValuesNotInclusion(ctx, e.Var, e.Values.Numbers, eb)
	} else {
		values, err = q.GetMatchingNumericValuesInclusion(ctx, e.Var, e.Values.Numbers, eb)
	}
	if err != nil {
		return nil, err
	}
	return reconstructAndORNumeric(ctx, q, block, e.Var, values)
}
