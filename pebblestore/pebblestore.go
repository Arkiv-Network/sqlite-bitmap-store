package pebblestore

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cockroachdb/pebble"
)

// PebbleStore implements persistent storage backed by PebbleDB.
type PebbleStore struct {
	db        *pebble.DB
	log       *slog.Logger
	idMu      sync.Mutex
	nextIDVal uint64
}

// NewPebbleStore opens (or creates) a PebbleDB database at dbPath and returns
// a ready-to-use PebbleStore. The existing ID counter is loaded from the
// database so that new IDs continue from where the previous run left off.
func NewPebbleStore(log *slog.Logger, dbPath string) (*PebbleStore, error) {
	opts := &pebble.Options{
		Levels: []pebble.LevelOptions{
			{Compression: pebble.SnappyCompression},
		},
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: open database: %w", err)
	}

	s := &PebbleStore{
		db:        db,
		log:       log,
		nextIDVal: 1,
	}

	// Load the persisted ID counter if it exists.
	val, closer, err := db.Get(idCounterKey())
	if err == nil {
		defer closer.Close()
		if len(val) == 8 {
			s.nextIDVal = binary.BigEndian.Uint64(val)
		}
	} else if err != pebble.ErrNotFound {
		_ = db.Close()
		return nil, fmt.Errorf("pebblestore: read id counter: %w", err)
	}

	log.Info("pebblestore opened", "path", dbPath, "nextID", s.nextIDVal)
	return s, nil
}

// DB returns the underlying PebbleDB instance for use as a pebble.Reader in
// tests and direct reads.
func (s *PebbleStore) DB() *pebble.DB {
	return s.db
}

// Close closes the underlying PebbleDB database.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// GetLastBlock returns the last processed block number, or 0 if none has been
// recorded yet.
func (s *PebbleStore) GetLastBlock() (uint64, error) {
	val, closer, err := s.db.Get(lastBlockKey())
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pebblestore: get last block: %w", err)
	}
	defer closer.Close()

	if len(val) != 8 {
		return 0, fmt.Errorf("pebblestore: last block value has unexpected length %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

// UpsertLastBlock writes the block number to the last-block key in the given
// batch.
func (s *PebbleStore) UpsertLastBlock(batch *pebble.Batch, block uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], block)
	return batch.Set(lastBlockKey(), buf[:], pebble.Sync)
}

// nextID atomically allocates a new unique ID and persists the updated counter
// to the provided batch. The caller must ensure the batch is eventually
// committed.
func (s *PebbleStore) nextID(batch *pebble.Batch) (uint64, error) {
	s.idMu.Lock()
	defer s.idMu.Unlock()

	id := s.nextIDVal
	s.nextIDVal++

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.nextIDVal)
	if err := batch.Set(idCounterKey(), buf[:], pebble.Sync); err != nil {
		// Roll back the in-memory counter on write failure.
		s.nextIDVal--
		return 0, fmt.Errorf("pebblestore: persist id counter: %w", err)
	}

	return id, nil
}
