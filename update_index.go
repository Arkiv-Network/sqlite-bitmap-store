package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

func (s *SQLiteStore) UpdateIndex(ctx context.Context, block uint64, update func(cache *bitmapCache, lastBlockFromDB uint64) error) error {

	tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	st := store.New(tx)

	lastBlockFromDB, err := st.GetLastBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get last block from database: %w", err)
	}

	cache := newBitmapCache(st, block)

	startTime := time.Now()

	err = update(cache, lastBlockFromDB)
	if err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}

	err = cache.Flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush cache: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.log.Info("index updated", "processingTime", time.Since(startTime).Milliseconds())

	return nil
}
