package sqlitebitmapstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
)

type SQLiteStore struct {
	writePool *sql.DB
	readPool  *sql.DB
	log       *slog.Logger
}

func NewSQLiteStore(
	log *slog.Logger,
	dbPath string,
	numberOfReadThreads int,
) (*SQLiteStore, error) {

	err := os.MkdirAll(filepath.Dir(dbPath), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	writeURL := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=immediate&_cache_size=65536", dbPath)

	writePool, err := sql.Open("sqlite3", writeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open write pool: %w", err)
	}

	readURL := fmt.Sprintf("file:%s?_query_only=true&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=deferred&_cache_size=65536", dbPath)
	readPool, err := sql.Open("sqlite3", readURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open read pool: %w", err)
	}

	readPool.SetMaxOpenConns(numberOfReadThreads)
	readPool.SetMaxIdleConns(numberOfReadThreads)
	readPool.SetConnMaxLifetime(0)
	readPool.SetConnMaxIdleTime(0)

	err = runMigrations(writePool)
	if err != nil {
		writePool.Close()
		readPool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &SQLiteStore{writePool: writePool, readPool: readPool, log: log}, nil
}

func runMigrations(db *sql.DB) error {
	sourceDriver, err := iofs.New(store.Migrations, "schema")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	dbDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	return s.writePool.Close()
}

func (s *SQLiteStore) GetLastBlock(ctx context.Context) (int64, error) {
	return store.New(s.writePool).GetLastBlock(ctx)
}

func (s *SQLiteStore) FollowEvents(ctx context.Context, iterator arkivevents.BatchIterator) error {

	for batch := range iterator {
		if batch.Error != nil {
			return fmt.Errorf("failed to follow events: %w", batch.Error)
		}

		err := func() error {

			tx, err := s.writePool.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSerializable,
				ReadOnly:  false,
			})
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			st := store.New(tx)

			firstBlock := batch.Batch.Blocks[0].Number
			lastBlock := batch.Batch.Blocks[len(batch.Batch.Blocks)-1].Number
			s.log.Info("new batch", "firstBlock", firstBlock, "lastBlock", lastBlock)

			lastBlockFromDB, err := st.GetLastBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to get last block from database: %w", err)
			}

		mainLoop:
			for _, block := range batch.Batch.Blocks {

				updates := 0
				deletes := 0
				extends := 0
				creates := 0
				ownerChanges := 0

				if block.Number <= uint64(lastBlockFromDB) {
					s.log.Info("skipping block", "block", block.Number, "lastBlockFromDB", lastBlockFromDB)
					continue mainLoop
				}

				updatesMap := map[common.Hash][]*events.OPUpdate{}

				for _, operation := range block.Operations {
					if operation.Update != nil {
						currentUpdates := updatesMap[operation.Update.Key]
						currentUpdates = append(currentUpdates, operation.Update)
						updatesMap[operation.Update.Key] = currentUpdates
					}
				}

				// blockNumber := block.Number
				for _, operation := range block.Operations {

					switch {

					case operation.Create != nil:
						// expiresAtBlock := blockNumber + operation.Create.BTL
						creates++
						key := operation.Create.Key

						stringAttributes := maps.Clone(operation.Create.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$creator"] = strings.ToLower(operation.Create.Owner.Hex())
						stringAttributes["$key"] = strings.ToLower(key.Hex())

						untilBlock := block.Number + operation.Create.BTL
						numericAttributes := maps.Clone(operation.Create.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = uint64(block.Number)

						sequence := block.Number<<32 | operation.TxIndex<<16 | operation.OpIndex
						numericAttributes["$sequence"] = sequence
						numericAttributes["$txIndex"] = uint64(operation.TxIndex)
						numericAttributes["$opIndex"] = uint64(operation.OpIndex)

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         operation.Create.Key.Bytes(),
								Payload:           operation.Create.Content,
								ContentType:       operation.Create.ContentType,
								StringAttributes:  stringAttributes,
								NumericAttributes: numericAttributes,
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload %s at block %d txIndex %d opIndex %d: %w", key.Hex(), block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						sbo := newStringBitmapOps(st)

						for k, v := range stringAttributes {
							err = sbo.Add(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						nbo := newNumericBitmapOps(st)

						for k, v := range numericAttributes {
							err = nbo.Add(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}
					case operation.Update != nil:
						updates++

						updates := updatesMap[operation.Update.Key]
						lastUpdate := updates[len(updates)-1]

						if operation.Update != lastUpdate {
							continue mainLoop
						}

						key := operation.Update.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldStringAttributes := latestPayload.StringAttributes

						oldNumericAttributes := latestPayload.NumericAttributes

						stringAttributes := maps.Clone(operation.Update.StringAttributes)

						stringAttributes["$owner"] = strings.ToLower(operation.Update.Owner.Hex())
						stringAttributes["$creator"] = oldStringAttributes["$creator"]
						stringAttributes["$key"] = strings.ToLower(operation.Update.Key.Hex())

						untilBlock := block.Number + operation.Update.BTL
						numericAttributes := maps.Clone(operation.Update.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = oldNumericAttributes["$createdAtBlock"]

						numericAttributes["$sequence"] = oldNumericAttributes["$sequence"]
						numericAttributes["$txIndex"] = oldNumericAttributes["$txIndex"]
						numericAttributes["$opIndex"] = oldNumericAttributes["$opIndex"]

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         key,
								Payload:           operation.Update.Content,
								ContentType:       operation.Update.ContentType,
								StringAttributes:  stringAttributes,
								NumericAttributes: numericAttributes,
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload 0x%x at block %d txIndex %d opIndex %d: %w", key, block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						sbo := newStringBitmapOps(st)

						for k, v := range oldStringAttributes {
							err = sbo.Remove(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						nbo := newNumericBitmapOps(st)

						for k, v := range oldNumericAttributes {
							err = nbo.Remove(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						// TODO: delete entity from the indexes

						for k, v := range stringAttributes {
							err = sbo.Add(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						for k, v := range numericAttributes {
							err = nbo.Add(ctx, k, v, id)
							if err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}

					case operation.Delete != nil || operation.Expire != nil:

						deletes++
						var key []byte
						if operation.Delete != nil {
							key = common.Hash(*operation.Delete).Bytes()
						} else {
							key = common.Hash(*operation.Expire).Bytes()
						}

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldStringAttributes := latestPayload.StringAttributes

						oldNumericAttributes := latestPayload.NumericAttributes

						sbo := newStringBitmapOps(st)

						for k, v := range oldStringAttributes {
							err = sbo.Remove(ctx, k, v, latestPayload.ID)
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						nbo := newNumericBitmapOps(st)

						for k, v := range oldNumericAttributes {
							err = nbo.Remove(ctx, k, v, latestPayload.ID)
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						err = st.DeletePayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to delete payload: %w", err)
						}

					case operation.ExtendBTL != nil:

						extends++

						key := operation.ExtendBTL.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldNumericAttributes := latestPayload.NumericAttributes

						newToBlock := block.Number + operation.ExtendBTL.BTL

						numericAttributes := maps.Clone(oldNumericAttributes)
						numericAttributes["$expiration"] = uint64(newToBlock)

						oldExpiration := oldNumericAttributes["$expiration"]

						id, err := st.UpsertPayload(ctx, store.UpsertPayloadParams{
							EntityKey:         key,
							Payload:           latestPayload.Payload,
							ContentType:       latestPayload.ContentType,
							StringAttributes:  latestPayload.StringAttributes,
							NumericAttributes: numericAttributes,
						})
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						nbo := newNumericBitmapOps(st)

						err = nbo.Remove(ctx, "$expiration", oldExpiration, id)
						if err != nil {
							return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
						}

						err = nbo.Add(ctx, "$expiration", newToBlock, id)
						if err != nil {
							return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
						}

					case operation.ChangeOwner != nil:
						ownerChanges++
						key := operation.ChangeOwner.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						stringAttributes := latestPayload.StringAttributes

						oldOwner := stringAttributes["$owner"]

						newOwner := strings.ToLower(operation.ChangeOwner.Owner.Hex())

						stringAttributes["$owner"] = newOwner

						id, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         key,
								Payload:           latestPayload.Payload,
								ContentType:       latestPayload.ContentType,
								StringAttributes:  stringAttributes,
								NumericAttributes: latestPayload.NumericAttributes,
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						sbo := newStringBitmapOps(st)

						err = sbo.Remove(ctx, "$owner", oldOwner, id)
						if err != nil {
							return fmt.Errorf("failed to remove string attribute value bitmap for owner: %w", err)
						}

						err = sbo.Add(ctx, "$owner", newOwner, id)
						if err != nil {
							return fmt.Errorf("failed to add string attribute value bitmap for owner: %w", err)
						}

					default:
						return fmt.Errorf("unknown operation: %v", operation)
					}

				}

				s.log.Info("block updated", "block", block.Number, "creates", creates, "updates", updates, "deletes", deletes, "extends", extends, "ownerChanges", ownerChanges)

			}

			err = st.UpsertLastBlock(ctx, int64(lastBlock))
			if err != nil {
				return fmt.Errorf("failed to upsert last block: %w", err)
			}

			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) NewQueries() *store.Queries {
	return store.New(s.readPool)
}
