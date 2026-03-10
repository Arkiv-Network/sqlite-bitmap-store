package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

// PruneBefore removes all historical data before block B.
// The most recent keyframes at or before B are preserved as the new base.
func (s *SQLiteStore) PruneBefore(ctx context.Context, block uint64) error {
	tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	st := store.New(tx)
	blockPtr := &block

	if err := st.PrunePayloadsBefore(ctx, blockPtr); err != nil {
		return fmt.Errorf("failed to prune payloads: %w", err)
	}
	if err := st.PruneStringBitmapsBefore(ctx, block); err != nil {
		return fmt.Errorf("failed to prune string bitmaps: %w", err)
	}
	if err := st.PruneNumericBitmapsBefore(ctx, block); err != nil {
		return fmt.Errorf("failed to prune numeric bitmaps: %w", err)
	}

	return tx.Commit()
}

// HandleReorg invalidates all data after block B. Payloads created after B are
// deleted, payloads closed after B are reopened, and bitmap entries after B are
// removed. The last_block is reset to B.
func (s *SQLiteStore) HandleReorg(ctx context.Context, block uint64) error {
	tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	st := store.New(tx)

	if err := st.ReorgReopenPayloads(ctx, block); err != nil {
		return fmt.Errorf("failed to reorg payloads: %w", err)
	}
	if err := st.ReorgDeleteNewPayloads(ctx, block); err != nil {
		return fmt.Errorf("failed to delete new payloads: %w", err)
	}
	if err := st.ReorgDeleteStringBitmaps(ctx, block); err != nil {
		return fmt.Errorf("failed to reorg string bitmaps: %w", err)
	}
	if err := st.ReorgDeleteNumericBitmaps(ctx, block); err != nil {
		return fmt.Errorf("failed to reorg numeric bitmaps: %w", err)
	}
	if err := st.UpsertLastBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to update last block: %w", err)
	}

	return tx.Commit()
}
