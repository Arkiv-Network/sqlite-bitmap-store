package store

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type NumericAttributes struct {
	Values map[string]uint64
}

func NewNumericAttributes(values map[string]uint64) *NumericAttributes {
	return &NumericAttributes{Values: values}
}

// Scanner interface for reading from DB
func (b *NumericAttributes) Scan(src any) error {

	if b.Values == nil {
		b.Values = make(map[string]uint64)
	}

	if src == nil {
		return nil
	}

	data, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}

	err := json.Unmarshal(data, &b)
	if err != nil {
		return fmt.Errorf("failed to unmarshal string attributes: %w", err)
	}

	return err
}

// Valuer interface for writing to DB
func (b *NumericAttributes) Value() (driver.Value, error) {
	if b.Values == nil {
		return nil, nil
	}

	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal string attributes: %w", err)
	}
	return data, nil
}
