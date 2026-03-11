package pebblestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
)

// PebbleStore implements bitmap storage backed by PebbleDB.
type PebbleStore struct {
	db  *pebble.DB
	log *slog.Logger

	idMu      sync.Mutex // protects nextIDVal
	nextIDVal uint64
}

// NewPebbleStore opens (or creates) a PebbleDB at dbPath and returns a ready
// store. The ID counter is loaded from the database so that nextID picks up
// where a previous run left off.
func NewPebbleStore(log *slog.Logger, dbPath string) (*PebbleStore, error) {
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return nil, fmt.Errorf("pebblestore: create directory: %w", err)
	}

	opts := &pebble.Options{
		Levels: []pebble.LevelOptions{
			{Compression: pebble.SnappyCompression},
		},
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: open db: %w", err)
	}

	s := &PebbleStore{
		db:  db,
		log: log,
	}

	// Load the persisted ID counter.
	val, closer, err := db.Get(idCounterKey())
	if err != nil && err != pebble.ErrNotFound {
		_ = db.Close()
		return nil, fmt.Errorf("pebblestore: load id counter: %w", err)
	}
	if err == nil {
		s.nextIDVal = binary.BigEndian.Uint64(val)
		_ = closer.Close()
	}
	// If ErrNotFound, nextIDVal stays at its zero value.

	return s, nil
}

// Close closes the underlying PebbleDB.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// GetLastBlock returns the most recently stored block number. If no block has
// been recorded yet, it returns 0 with a nil error.
func (s *PebbleStore) GetLastBlock(ctx context.Context) (uint64, error) {
	val, closer, err := s.db.Get(lastBlockKey())
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pebblestore: get last block: %w", err)
	}
	defer closer.Close()

	return binary.BigEndian.Uint64(val), nil
}

// UpsertLastBlock writes the given block number into the batch.
func (s *PebbleStore) UpsertLastBlock(batch *pebble.Batch, block uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], block)
	return batch.Set(lastBlockKey(), buf[:], pebble.Sync)
}

// nextID increments the internal ID counter, persists it to the batch, and
// returns the new value. It is safe for concurrent use.
func (s *PebbleStore) nextID(batch *pebble.Batch) (uint64, error) {
	s.idMu.Lock()
	defer s.idMu.Unlock()

	s.nextIDVal++

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.nextIDVal)
	if err := batch.Set(idCounterKey(), buf[:], pebble.Sync); err != nil {
		s.nextIDVal-- // roll back on failure
		return 0, fmt.Errorf("pebblestore: persist id counter: %w", err)
	}

	return s.nextIDVal, nil
}
