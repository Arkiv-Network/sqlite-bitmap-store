package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

const MaxPoolCount int = 128
const MaxTxCount int = 7

type HistoricTransaction struct {
	tx    *sql.Tx
	pool  *historicTransactionAtBlockPool
	inUse bool
}

func (h HistoricTransaction) AtBlock() uint64 {
	return h.pool.block
}

func (h HistoricTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if !h.inUse {
		return nil, fmt.Errorf("historic transaction has been returned to the pool")
	}
	return h.tx.ExecContext(ctx, query, args...)
}

func (h HistoricTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if !h.inUse {
		return nil, fmt.Errorf("historic transaction has been returned to the pool")
	}
	return h.tx.PrepareContext(ctx, query)
}

func (h HistoricTransaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if !h.inUse {
		return nil, fmt.Errorf("historic transaction has been returned to the pool")
	}
	return h.tx.QueryContext(ctx, query, args...)
}

func (h HistoricTransaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// This calls QueryContext internally, so no need to check explicitly here whether
	// the tx has been returned.
	// In any case we also cannot construct an sql.Row struct here, since its fields
	// are not exported.
	return h.tx.QueryRowContext(ctx, query, args...)
}

var _ store.DBTX = HistoricTransaction{}

func (h *HistoricTransaction) rollback() error {
	if err := h.tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("error rolling back historic transaction: %w", err)
	}
	return nil
}

func (h *HistoricTransaction) Close() error {
	return h.pool.returnTx(h)
}

type historicTransactionAtBlockPool struct {
	block    uint64
	mu       sync.Mutex
	returned *sync.Cond
	txs      []*HistoricTransaction
	closed   bool
	log      *slog.Logger
}

func (h *historicTransactionAtBlockPool) getTransaction(ctx context.Context) (*HistoricTransaction, error) {
	h.returned.L.Lock()
	defer h.returned.L.Unlock()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tx, err := func() (*HistoricTransaction, error) {
			h.mu.Lock()
			defer h.mu.Unlock()

			if h.closed {
				return nil, fmt.Errorf("pool is closed")
			}

			if len(h.txs) > 0 {
				tx := h.txs[0]
				h.txs = h.txs[1:]
				tx.inUse = true
				return tx, nil
			}

			return nil, nil
		}()

		if err != nil {
			return nil, err
		}

		if tx != nil {
			return tx, nil
		}

		h.returned.Wait()
	}
}

func (h *historicTransactionAtBlockPool) returnTx(tx *HistoricTransaction) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tx.inUse = false

	if h.closed {
		if err := tx.rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			return err
		}
	} else {
		// We always create a new HistoricTransaction so that we never set
		// returned = false on a HistoricTransaction that we've ever given away
		h.txs = append(h.txs, &HistoricTransaction{
			tx:   tx.tx,
			pool: h,
		})
		h.returned.Signal()
	}

	return nil
}

func (h *historicTransactionAtBlockPool) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	errs := []error{}
	for _, tx := range h.txs {
		if err := tx.rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			errs = append(errs, err)
		}
	}
	h.closed = true
	return errors.Join(errs...)
}

type HistoricTransactionPool struct {
	readPool *sql.DB

	// Protect the array below
	mu    sync.Mutex
	pools []*historicTransactionAtBlockPool

	log *slog.Logger
}

func NewHistoricTransactionPool(readPool *sql.DB, log *slog.Logger) *HistoricTransactionPool {
	pools := make([]*historicTransactionAtBlockPool, 0, MaxPoolCount)

	return &HistoricTransactionPool{
		readPool: readPool,
		pools:    pools,
		log:      log,
	}
}

func (h *HistoricTransactionPool) newPoolAtBlock(block uint64) (*historicTransactionAtBlockPool, error) {
	txs := make([]*HistoricTransaction, 0, MaxTxCount)
	pool := historicTransactionAtBlockPool{
		block:    block,
		txs:      txs,
		log:      h.log,
		returned: sync.NewCond(&sync.Mutex{}),
	}

	b := uint64(0)
	for range MaxTxCount {
		histTx, err := h.readPool.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: sql.LevelSerializable,
			ReadOnly:  true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to start historic read transaction: %w", err)
		}

		// We need to run a query in order for the transaction to actually refer to the
		// correct end mark in the sqlite WAL.
		store := store.New(histTx)
		b, err = store.GetLastBlock(context.Background())
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("failed to start historic read transaction: %w", err),
				histTx.Rollback(),
				pool.Close(),
			)
		}
		if b != block {
			return nil, errors.Join(
				fmt.Errorf("failed to start tx at right block, got %d, expected %d", b, block),
				histTx.Rollback(),
				pool.Close(),
			)
		}

		pool.txs = append(pool.txs, &HistoricTransaction{
			tx:   histTx,
			pool: &pool,
		})
	}
	h.log.Info("created historic read transactions", "count", MaxTxCount, "at_block", b)

	return &pool, nil
}

func (h *HistoricTransactionPool) GetTransaction(ctx context.Context, atBlock uint64) (*HistoricTransaction, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	lastBlock, err := store.New(h.readPool).GetLastBlock(ctx)
	if err != nil {
		return nil, err
	}

	if atBlock > lastBlock {
		return nil, fmt.Errorf("block in the future")
	}

	numOfPools := len(h.pools)
	if numOfPools == 0 {
		return nil, fmt.Errorf("the historic transaction pool is empty")
	}

	oldestAvailableBlock := lastBlock - (uint64(numOfPools) - 1)

	if atBlock < oldestAvailableBlock {
		return nil, fmt.Errorf("requested block is no longer available, requested: %d, oldest available: %d", atBlock, oldestAvailableBlock)
	}

	offset := lastBlock - atBlock
	txIx := numOfPools - 1 - int(offset)

	return h.pools[txIx].getTransaction(ctx)
}

// CommitTxAndCreatePoolAtBlock commits the given transaction and creates a new
// pool of read transactions. It's important to do both at once while locking
// the HistoricTransactionPool to ensure that no other go-routine can get
// a transaction from the pool after the transaction was committed but before
// the new pool of read transactions for the new block has been added.
func (h *HistoricTransactionPool) CommitTxAndCreatePoolAtBlock(block uint64, tx *sql.Tx) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	err := tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	pool, err := h.newPoolAtBlock(block)
	if err != nil {
		return fmt.Errorf("failed to create historic read transaction pool: %w", err)
	}

	for len(h.pools) >= MaxPoolCount {
		oldPool := h.pools[0]
		h.pools = h.pools[1:]
		if err := oldPool.Close(); err != nil {
			return fmt.Errorf("failed to close discarded historic read transaction pool: %w", err)
		}
	}
	h.pools = append(h.pools, pool)

	h.log.Info("historic read transaction pool", "size", len(h.pools))
	return nil
}
