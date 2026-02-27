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
	"time"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
	_ "github.com/dolthub/driver"
	"github.com/ethereum/go-ethereum/common"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"

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

	absPath, err := filepath.Abs(filepath.Clean(dbPath))
	if err != nil {
		return nil, err
	}
	dbPath = absPath
	log.Info("dbPath", "dbPath", dbPath)

	// if exists check if it is a directory
	if info, err := os.Stat(dbPath); err == nil && !info.IsDir() {
		return nil, fmt.Errorf("dbPath is not a directory: %w", err)
	}

	err = os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	//writeURL := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=11000&_journal_mode=WAL&_auto_vacuum=incremental&_foreign_keys=true&_txlock=immediate&_cache_size=65536", dbPath)
	writeURL := fmt.Sprintf("file://%s?commitname=arkiv&commitemail=arkiv@arkiv.network&database=arkiv", dbPath)
	log.Info("writeURL", "writeURL", writeURL)

	writePool, err := sql.Open("dolt", writeURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open write pool: %w", err)
	}
	_, err = writePool.ExecContext(context.Background(), "CREATE DATABASE IF NOT EXISTS arkiv;")
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}
	log.Info("database created", "database", "arkiv")

	readURL := fmt.Sprintf("file://%s?commitname=arkiv&commitemail=arkiv@arkiv.network&database=arkiv", dbPath)
	log.Info("readURL", "readURL", readURL)
	readPool, err := sql.Open("dolt", readURL)
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

	dbDriver, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "mysql", dbDriver)
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

func (s *SQLiteStore) GetLastBlock(ctx context.Context) (uint64, error) {
	return store.New(s.writePool).GetLastBlock(ctx)
}

func (s *SQLiteStore) FollowEvents(ctx context.Context, iterator arkivevents.BatchIterator) error {

	for batch := range iterator {
		if batch.Error != nil {
			return fmt.Errorf("failed to follow events: %w", batch.Error)
		}

		totalCreates := 0
		totalUpdates := 0
		totalDeletes := 0
		totalExtends := 0
		totalOwnerChanges := 0

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

			cache := newBitmapCache(st)

			startTime := time.Now()
			startOperationsTime := time.Now()

		mainLoop:
			for _, block := range batch.Batch.Blocks {

				type pendingCreate struct {
					entityKey        []byte
					payload          []byte
					contentType      string
					stringAttributes map[string]string
					numericAttributes map[string]uint64
				}

				pendingCreates := make(map[string]pendingCreate)
				pendingCreateOrder := make([]string, 0)

				flushPendingCreates := func() error {
					if len(pendingCreateOrder) == 0 {
						return nil
					}

					upserts := make([]store.UpsertPayloadParams, 0, len(pendingCreateOrder))
					entityKeys := make([][]byte, 0, len(pendingCreateOrder))
					for _, k := range pendingCreateOrder {
						pc := pendingCreates[k]
						upserts = append(upserts, store.UpsertPayloadParams{
							EntityKey:         pc.entityKey,
							Payload:           pc.payload,
							ContentType:       pc.contentType,
							StringAttributes:  store.NewStringAttributes(pc.stringAttributes),
							NumericAttributes: store.NewNumericAttributes(pc.numericAttributes),
						})
						entityKeys = append(entityKeys, pc.entityKey)
					}

					if err := st.BulkUpsertPayloads(ctx, upserts); err != nil {
						return fmt.Errorf("failed to bulk upsert payloads: %w", err)
					}

					idsByKey, err := st.GetPayloadIDsForEntityKeys(ctx, entityKeys)
					if err != nil {
						return fmt.Errorf("failed to get payload ids for keys: %w", err)
					}

					for _, k := range pendingCreateOrder {
						pc := pendingCreates[k]
						id, ok := idsByKey[string(pc.entityKey)]
						if !ok {
							return fmt.Errorf("failed to find id for entity key")
						}

						for sk, sv := range pc.stringAttributes {
							if err := cache.AddToStringBitmap(ctx, sk, sv, id); err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						for nk, nv := range pc.numericAttributes {
							switch nk {
							case "$txIndex", "$opIndex":
								continue
							}
							if err := cache.AddToNumericBitmap(ctx, nk, nv, id); err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}
					}

					clear(pendingCreates)
					pendingCreateOrder = pendingCreateOrder[:0]
					return nil
				}

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
			operationLoop:
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
						numericAttributes["$lastModifiedAtBlock"] = uint64(block.Number)

						sequence := block.Number<<32 | operation.TxIndex<<16 | operation.OpIndex
						numericAttributes["$sequence"] = sequence
						numericAttributes["$txIndex"] = uint64(operation.TxIndex)
						numericAttributes["$opIndex"] = uint64(operation.OpIndex)

						ek := operation.Create.Key.Bytes()
						mapKey := string(ek)
						if _, exists := pendingCreates[mapKey]; !exists {
							pendingCreateOrder = append(pendingCreateOrder, mapKey)
						}
						pendingCreates[mapKey] = pendingCreate{
							entityKey:        ek,
							payload:          operation.Create.Content,
							contentType:      operation.Create.ContentType,
							stringAttributes: stringAttributes,
							numericAttributes: numericAttributes,
						}

					case operation.Update != nil:
						if err := flushPendingCreates(); err != nil {
							return err
						}
						updates++

						updates := updatesMap[operation.Update.Key]
						lastUpdate := updates[len(updates)-1]

						if operation.Update != lastUpdate {
							continue operationLoop
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
						stringAttributes["$creator"] = oldStringAttributes.Values["$creator"]
						stringAttributes["$key"] = strings.ToLower(operation.Update.Key.Hex())

						untilBlock := block.Number + operation.Update.BTL
						numericAttributes := maps.Clone(operation.Update.NumericAttributes)
						numericAttributes["$expiration"] = uint64(untilBlock)
						numericAttributes["$createdAtBlock"] = oldNumericAttributes.Values["$createdAtBlock"]

						numericAttributes["$sequence"] = oldNumericAttributes.Values["$sequence"]
						numericAttributes["$txIndex"] = oldNumericAttributes.Values["$txIndex"]
						numericAttributes["$opIndex"] = oldNumericAttributes.Values["$opIndex"]
						numericAttributes["$lastModifiedAtBlock"] = uint64(block.Number)

						result, err := st.UpsertPayload(
							ctx,
							store.UpsertPayloadParams{
								EntityKey:         key,
								Payload:           operation.Update.Content,
								ContentType:       operation.Update.ContentType,
								StringAttributes:  store.NewStringAttributes(stringAttributes),
								NumericAttributes: store.NewNumericAttributes(numericAttributes),
							},
						)
						if err != nil {
							return fmt.Errorf("failed to insert payload 0x%x at block %d txIndex %d opIndex %d: %w", key, block.Number, operation.TxIndex, operation.OpIndex, err)
						}
						id, err := result.LastInsertId()
						if err != nil {
							return fmt.Errorf("failed to get last insert id: %w", err)
						}

						for k, v := range oldStringAttributes.Values {
							err = cache.RemoveFromStringBitmap(ctx, k, v, uint64(id))
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						for k, v := range oldNumericAttributes.Values {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.RemoveFromNumericBitmap(ctx, k, v, uint64(id))
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						// TODO: delete entity from the indexes

						for k, v := range stringAttributes {
							err = cache.AddToStringBitmap(ctx, k, v, uint64(id))
							if err != nil {
								return fmt.Errorf("failed to add string attribute value bitmap: %w", err)
							}
						}

						for k, v := range numericAttributes {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.AddToNumericBitmap(ctx, k, v, uint64(id))
							if err != nil {
								return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
							}
						}

					case operation.Delete != nil || operation.Expire != nil:
						if err := flushPendingCreates(); err != nil {
							return err
						}

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

						for k, v := range oldStringAttributes.Values {
							err = cache.RemoveFromStringBitmap(ctx, k, v, uint64(latestPayload.ID))
							if err != nil {
								return fmt.Errorf("failed to remove string attribute value bitmap: %w", err)
							}
						}

						for k, v := range oldNumericAttributes.Values {
							// skip txIndex and opIndex because they are not used for querying
							switch k {
							case "$txIndex", "$opIndex":
								continue
							}

							err = cache.RemoveFromNumericBitmap(ctx, k, v, uint64(latestPayload.ID))
							if err != nil {
								return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
							}
						}

						err = st.DeletePayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to delete payload: %w", err)
						}

					case operation.ExtendBTL != nil:
						if err := flushPendingCreates(); err != nil {
							return err
						}

						extends++

						key := operation.ExtendBTL.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						oldNumericAttributes := latestPayload.NumericAttributes

						oldExpiration := oldNumericAttributes.Values["$expiration"]

						newToBlock := oldExpiration + operation.ExtendBTL.BTL

						numericAttributes := maps.Clone(oldNumericAttributes.Values)
						numericAttributes["$expiration"] = uint64(newToBlock)

						result, err := st.UpsertPayload(ctx, store.UpsertPayloadParams{
							EntityKey:         key,
							Payload:           latestPayload.Payload,
							ContentType:       latestPayload.ContentType,
							StringAttributes:  latestPayload.StringAttributes,
							NumericAttributes: store.NewNumericAttributes(numericAttributes),
						})
						if err != nil {
							return fmt.Errorf("failed to insert payload at block %d txIndex %d opIndex %d: %w", block.Number, operation.TxIndex, operation.OpIndex, err)
						}

						id, err := result.LastInsertId()
						if err != nil {
							return fmt.Errorf("failed to get last insert id: %w", err)
						}

						err = cache.RemoveFromNumericBitmap(ctx, "$expiration", oldExpiration, uint64(id))
						if err != nil {
							return fmt.Errorf("failed to remove numeric attribute value bitmap: %w", err)
						}

						err = cache.AddToNumericBitmap(ctx, "$expiration", newToBlock, uint64(id))
						if err != nil {
							return fmt.Errorf("failed to add numeric attribute value bitmap: %w", err)
						}

					case operation.ChangeOwner != nil:
						if err := flushPendingCreates(); err != nil {
							return err
						}
						ownerChanges++
						key := operation.ChangeOwner.Key.Bytes()

						latestPayload, err := st.GetPayloadForEntityKey(ctx, key)
						if err != nil {
							return fmt.Errorf("failed to get latest payload: %w", err)
						}

						stringAttributes := latestPayload.StringAttributes

						oldOwner := stringAttributes.Values["$owner"]

						newOwner := strings.ToLower(operation.ChangeOwner.Owner.Hex())

						stringAttributes.Values["$owner"] = newOwner

						result, err := st.UpsertPayload(
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

						id, err := result.LastInsertId()
						if err != nil {
							return fmt.Errorf("failed to get last insert id: %w", err)
						}

						err = cache.RemoveFromStringBitmap(ctx, "$owner", oldOwner, uint64(id))
						if err != nil {
							return fmt.Errorf("failed to remove string attribute value bitmap for owner: %w", err)
						}

						err = cache.AddToStringBitmap(ctx, "$owner", newOwner, uint64(id))
						if err != nil {
							return fmt.Errorf("failed to add string attribute value bitmap for owner: %w", err)
						}

					default:
						if err := flushPendingCreates(); err != nil {
							return err
						}
						return fmt.Errorf("unknown operation: %v", operation)
					}

				}

				if err := flushPendingCreates(); err != nil {
					return err
				}

				s.log.Info("block updated", "block", block.Number, "creates", creates, "updates", updates, "deletes", deletes, "extends", extends, "ownerChanges", ownerChanges)
				totalCreates += creates
				totalUpdates += updates
				totalDeletes += deletes
				totalExtends += extends
				totalOwnerChanges += ownerChanges
			}
			operationsProcessingTime := time.Since(startOperationsTime).Milliseconds()
			s.log.Info("operations processed", "operationsProcessingTime", operationsProcessingTime)

			err = st.UpsertLastBlock(ctx, lastBlock)
			if err != nil {
				return fmt.Errorf("failed to upsert last block: %w", err)
			}

			flushBitmapCacheTime := time.Now()
			err = cache.Flush(ctx)
			if err != nil {
				return fmt.Errorf("failed to flush bitmap cache: %w", err)
			}
			flushBitmapCacheProcessingTime := time.Since(flushBitmapCacheTime).Milliseconds()
			s.log.Info("bitmap cache flushed", "flushBitmapCacheProcessingTime", flushBitmapCacheProcessingTime)

			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			tx, err = s.writePool.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSerializable,
				ReadOnly:  false,
			})
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			st = store.New(tx)

			doltCommitTime := time.Now()
			err = st.DoltAdd(ctx)
			if err != nil {
				return fmt.Errorf("failed to add to dolt: %w", err)
			}

			err = st.DoltCommit(ctx, fmt.Sprintf("Batch processed from block %d to %d", firstBlock, lastBlock))
			if err != nil {
				// Dolt returns "nothing to commit" when there were no DB changes.
				// This can happen when we skip already-processed blocks.
				if !strings.Contains(err.Error(), "nothing to commit") {
					return fmt.Errorf("failed to commit dolt: %w", err)
				}
			}
			doltProcessingTime := time.Since(doltCommitTime).Milliseconds()
			s.log.Info("dolt committed", "doltProcessingTime", doltProcessingTime)

			s.log.Info(
				"batch processed",
				"firstBlock", firstBlock,
				"lastBlock", lastBlock,
				"processingTime", time.Since(startTime).Milliseconds(),
				"creates", totalCreates,
				"updates", totalUpdates,
				"deletes", totalDeletes,
				"extends", totalExtends,
				"ownerChanges", totalOwnerChanges,
				"operationsProcessingTime", operationsProcessingTime,
				"flushBitmapCacheProcessingTime", flushBitmapCacheProcessingTime,
				"doltProcessingTime", doltProcessingTime,
			)

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

func (s *SQLiteStore) ReadTransaction(ctx context.Context, fn func(q *store.Queries) error) error {
	tx, err := s.readPool.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	st := store.New(tx)

	return fn(st)
}

func (s *SQLiteStore) GetNumberOfEntities(ctx context.Context) (numberOfEntities uint64, err error) {
	err = s.ReadTransaction(ctx, func(q *store.Queries) error {
		ni, err := q.GetNumberOfEntities(ctx)
		if err != nil {
			return fmt.Errorf("failed to get number of entities: %w", err)
		}
		numberOfEntities = uint64(ni)
		return nil
	})

	if err != nil {
		return 0, err
	}

	return numberOfEntities, nil
}
