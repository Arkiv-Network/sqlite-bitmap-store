package store

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type StringAttributes struct {
	Values map[string]string
}

func NewStringAttributes(values map[string]string) *StringAttributes {
	return &StringAttributes{Values: values}
}

// Scanner interface for reading from DB
func (b *StringAttributes) Scan(src any) error {
	if b.Values == nil {
		b.Values = make(map[string]string)
	}

	if src == nil {
		return nil
	}

	var data []byte

	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return fmt.Errorf("expected string or []byte, got %T", src)
	}

	err := json.Unmarshal(data, &b)
	if err != nil {
		return fmt.Errorf("failed to unmarshal string attributes: %w", err)
	}

	return err
}

// Valuer interface for writing to DB
func (b *StringAttributes) Value() (driver.Value, error) {
	if b == nil {
		return nil, nil
	}

	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal string attributes: %w", err)
	}
	return data, nil
}
