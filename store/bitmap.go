package store

import (
	"bytes"
	"database/sql/driver"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type Bitmap struct {
	*roaring64.Bitmap
}

func NewBitmap() *Bitmap {
	return &Bitmap{Bitmap: roaring64.New()}
}

// Scanner interface for reading from DB
func (b *Bitmap) Scan(src any) error {
	if src == nil {
		b.Bitmap = roaring64.New()
		return nil
	}

	data, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}

	b.Bitmap = roaring64.New()
	err := b.Bitmap.UnmarshalBinary(data)
	return err
}

// Valuer interface for writing to DB
func (b *Bitmap) Value() (driver.Value, error) {
	if b.Bitmap == nil {
		return nil, nil
	}

	buf := new(bytes.Buffer)
	_, err := b.Bitmap.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
