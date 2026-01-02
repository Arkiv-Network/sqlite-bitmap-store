package query

import (
	"strings"
)

func (t *TopLevel) Normalize() *TopLevel {
	if t.Expression != nil {
		return &TopLevel{
			Expression: t.Expression.Normalize(),
		}
	}
	return t
}

func (e *Expression) Normalize() *Expression {
	normalised := e.Or.Normalize()
	// Remove unneeded OR+AND nodes that both only contain a single child
	// when that child is a parenthesised expression
	if len(normalised.Right) == 0 && len(normalised.Left.Right) == 0 && normalised.Left.Left.Paren != nil {
		// This has already been normalised by the call above, so any negation has
		// been pushed into the leaf expressions and we can safely strip away the
		// parentheses
		return &normalised.Left.Left.Paren.Nested
	}
	return &Expression{
		Or: *normalised,
	}
}

func (e *Expression) invert() *Expression {

	newLeft := e.Or.invert()

	if len(newLeft.Right) == 0 {
		// By construction, this will always be a Paren
		if newLeft.Left.Paren == nil {
			panic("This should never happen!")
		}
		return &newLeft.Left.Paren.Nested
	}

	return &Expression{
		Or: OrExpression{
			Left: *newLeft,
		},
	}
}

func (e *OrExpression) Normalize() *OrExpression {
	var newRight []*OrRHS = nil

	if e.Right != nil {
		newRight = make([]*OrRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.Normalize())
		}
	}

	return &OrExpression{
		Left:  *e.Left.Normalize(),
		Right: newRight,
	}
}

func (e *OrExpression) invert() *AndExpression {
	newLeft := EqualExpr{
		Paren: &Paren{
			IsNot: false,
			Nested: Expression{
				Or: *e.Left.invert(),
			},
		},
	}

	var newRight []*AndRHS = nil

	if e.Right != nil {
		newRight = make([]*AndRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &AndExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

func (e *OrRHS) Normalize() *OrRHS {
	return &OrRHS{
		Expr: *e.Expr.Normalize(),
	}
}

func (e *OrRHS) invert() *AndRHS {
	return &AndRHS{
		Expr: EqualExpr{
			Paren: &Paren{
				IsNot: false,
				Nested: Expression{
					Or: *e.Expr.invert(),
				},
			},
		},
	}
}

func (e *AndExpression) Normalize() *AndExpression {
	var newRight []*AndRHS = nil

	if e.Right != nil {
		newRight = make([]*AndRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.Normalize())
		}
	}

	return &AndExpression{
		Left:  *e.Left.Normalize(),
		Right: newRight,
	}
}

func (e *AndExpression) invert() *OrExpression {
	newLeft := AndExpression{
		Left: *e.Left.invert(),
	}

	var newRight []*OrRHS = nil

	if e.Right != nil {
		newRight = make([]*OrRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &OrExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

func (e *AndRHS) Normalize() *AndRHS {
	return &AndRHS{
		Expr: *e.Expr.Normalize(),
	}
}

func (e *AndRHS) invert() *OrRHS {
	return &OrRHS{
		Expr: AndExpression{
			Left: *e.Expr.invert(),
		},
	}
}

func (e *EqualExpr) Normalize() *EqualExpr {

	if e.Paren != nil {
		p := e.Paren.Normalize()

		// Remove parentheses that only contain a single nested expression
		// (i.e. no OR or AND with multiple children)
		if len(p.Nested.Or.Right) == 0 && len(p.Nested.Or.Left.Right) == 0 {
			// This expression should already be properly normalised, we don't need to
			// call Normalise again here
			return &p.Nested.Or.Left.Left
		} else {
			return &EqualExpr{Paren: p}
		}
	}

	if e.LessThan != nil {
		return &EqualExpr{LessThan: e.LessThan.Normalize()}
	}

	if e.LessOrEqualThan != nil {
		return &EqualExpr{LessOrEqualThan: e.LessOrEqualThan.Normalize()}
	}

	if e.GreaterThan != nil {
		return &EqualExpr{GreaterThan: e.GreaterThan.Normalize()}
	}

	if e.GreaterOrEqualThan != nil {
		return &EqualExpr{GreaterOrEqualThan: e.GreaterOrEqualThan.Normalize()}
	}

	if e.Glob != nil {
		return &EqualExpr{Glob: e.Glob.Normalize()}
	}

	if e.Assign != nil {
		return &EqualExpr{Assign: e.Assign.Normalize()}
	}

	if e.Inclusion != nil {
		return &EqualExpr{Inclusion: e.Inclusion.Normalize()}
	}

	panic("This should not happen!")
}

func (e *EqualExpr) invert() *EqualExpr {
	if e.Paren != nil {
		return &EqualExpr{Paren: e.Paren.invert()}
	}

	if e.LessThan != nil {
		return &EqualExpr{GreaterOrEqualThan: e.LessThan.invert()}
	}

	if e.LessOrEqualThan != nil {
		return &EqualExpr{GreaterThan: e.LessOrEqualThan.invert()}
	}

	if e.GreaterThan != nil {
		return &EqualExpr{LessOrEqualThan: e.GreaterThan.invert()}
	}

	if e.GreaterOrEqualThan != nil {
		return &EqualExpr{LessThan: e.GreaterOrEqualThan.invert()}
	}

	if e.Glob != nil {
		return &EqualExpr{Glob: e.Glob.invert()}
	}

	if e.Assign != nil {
		return &EqualExpr{Assign: e.Assign.invert()}
	}

	if e.Inclusion != nil {
		return &EqualExpr{Inclusion: e.Inclusion.invert()}
	}

	panic("This should not happen!")
}

func (e *Paren) Normalize() *Paren {
	nested := e.Nested

	if e.IsNot {
		nested = *nested.invert()
	}

	return &Paren{
		IsNot:  false,
		Nested: *nested.Normalize(),
	}
}

func (e *Paren) invert() *Paren {
	return &Paren{
		IsNot:  !e.IsNot,
		Nested: e.Nested,
	}
}

func (e *Glob) Normalize() *Glob {
	// TODO do we need to change casing here too?
	return e
}

func (e *Glob) invert() *Glob {
	return &Glob{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

func (e *LessThan) Normalize() *LessThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &LessThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *LessThan) invert() *GreaterOrEqualThan {
	return &GreaterOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *LessOrEqualThan) Normalize() *LessOrEqualThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &LessOrEqualThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *LessOrEqualThan) invert() *GreaterThan {
	return &GreaterThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *GreaterThan) Normalize() *GreaterThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &GreaterThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *GreaterThan) invert() *LessOrEqualThan {
	return &LessOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *GreaterOrEqualThan) Normalize() *GreaterOrEqualThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &GreaterOrEqualThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *GreaterOrEqualThan) invert() *LessThan {
	return &LessThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *Equality) Normalize() *Equality {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &Equality{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *Equality) invert() *Equality {
	return &Equality{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

func (e *Inclusion) Normalize() *Inclusion {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		vals := make([]string, 0, len(e.Values.Strings))
		for _, val := range e.Values.Strings {
			vals = append(vals, strings.ToLower(val))
		}
		return &Inclusion{
			Var: e.Var,
			Values: Values{
				Strings: vals,
			},
		}
	default:
		return e
	}
}

func (e *Inclusion) invert() *Inclusion {
	return &Inclusion{
		Var:    e.Var,
		IsNot:  !e.IsNot,
		Values: e.Values,
	}
}
