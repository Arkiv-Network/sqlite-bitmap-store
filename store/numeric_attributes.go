package store

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type NumericAttributes map[string]uint64

// Scanner interface for reading from DB
func (b NumericAttributes) Scan(src any) error {
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
func (b NumericAttributes) Value() (driver.Value, error) {
	if b == nil {
		return nil, nil
	}

	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal string attributes: %w", err)
	}
	return data, nil
}
