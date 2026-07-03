package sqlitebitmapstore_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
	sqlitebitmapstore "github.com/Arkiv-Network/sqlite-bitmap-store"
	"github.com/Arkiv-Network/sqlite-bitmap-store/pusher"
	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

var _ = Describe("SQLiteStore", func() {
	var (
		sqlStore *sqlitebitmapstore.SQLiteStore
		tmpDir   string
		ctx      context.Context
		cancel   context.CancelFunc
		logger   *slog.Logger
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "sqlitestore_test")
		Expect(err).NotTo(HaveOccurred())

		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
		dbPath := filepath.Join(tmpDir, "test.db")

		sqlStore, err = sqlitebitmapstore.NewSQLiteStore(logger, dbPath, 4)
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		cancel()
		if sqlStore != nil {
			sqlStore.Close()
		}
		os.RemoveAll(tmpDir)
	})

	Describe("FollowEvents with batch of two blocks", func() {
		It("should insert data and allow querying by string and numeric attributes", func() {
			iterator := pusher.NewPushIterator()

			key1 := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
			key2 := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			batch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key1,
									ContentType: "application/json",
									BTL:         1000,
									Owner:       owner,
									Content:     []byte(`{"name": "document1"}`),
									StringAttributes: map[string]string{
										"type":     "document",
										"category": "reports",
									},
									NumericAttributes: map[string]uint64{
										"version":  1,
										"priority": 10,
									},
								},
							},
						},
					},
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key2,
									ContentType: "image/png",
									BTL:         2000,
									Owner:       owner,
									Content:     []byte(`image data`),
									StringAttributes: map[string]string{
										"type":     "image",
										"category": "media",
									},
									NumericAttributes: map[string]uint64{
										"version":  2,
										"priority": 20,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			lastBlock, err := sqlStore.GetLastBlock(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(lastBlock).To(Equal(uint64(101)))

			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				// Query by string attribute: type = "document"
				docBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "type",
					Value: "document",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(docBitmap).NotTo(BeNil())

				docIDs := docBitmap.ToArray()
				Expect(docIDs).To(HaveLen(1))

				docPayloads, err := q.RetrievePayloads(ctx, docIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(docPayloads).To(HaveLen(1))
				Expect(docPayloads[0].Payload).To(Equal([]byte(`{"name": "document1"}`)))
				Expect(docPayloads[0].ContentType).To(Equal("application/json"))
				Expect(docPayloads[0].StringAttributes.Values["type"]).To(Equal("document"))

				// Query by string attribute: type = "image"
				imageBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "type",
					Value: "image",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(imageBitmap).NotTo(BeNil())

				imageIDs := imageBitmap.ToArray()
				Expect(imageIDs).To(HaveLen(1))

				imagePayloads, err := q.RetrievePayloads(ctx, imageIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(imagePayloads).To(HaveLen(1))
				Expect(imagePayloads[0].Payload).To(Equal([]byte(`image data`)))
				Expect(imagePayloads[0].ContentType).To(Equal("image/png"))

				// Query by numeric attribute: version = 1
				version1Bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "version",
					Value: store.NumericValueToSQL(1),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(version1Bitmap).NotTo(BeNil())

				version1IDs := version1Bitmap.ToArray()
				Expect(version1IDs).To(HaveLen(1))

				version1Payloads, err := q.RetrievePayloads(ctx, version1IDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(version1Payloads).To(HaveLen(1))
				Expect(version1Payloads[0].NumericAttributes.Values["version"]).To(Equal(uint64(1)))

				// Query by numeric attribute: version > 1
				versionGT1Bitmaps, err := q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams{
					Name:  "version",
					Value: store.NumericValueToSQL(1),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(versionGT1Bitmaps).To(HaveLen(1))

				// Combine bitmaps to get all IDs with version > 1
				combinedBitmap := store.NewBitmap()
				for _, bm := range versionGT1Bitmaps {
					combinedBitmap.Or(bm.Bitmap)
				}

				versionGT1IDs := combinedBitmap.ToArray()
				Expect(versionGT1IDs).To(HaveLen(1))

				versionGT1Payloads, err := q.RetrievePayloads(ctx, versionGT1IDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(versionGT1Payloads).To(HaveLen(1))
				Expect(versionGT1Payloads[0].NumericAttributes.Values["version"]).To(Equal(uint64(2)))

				// Query by numeric attribute: priority >= 10
				priorityGTE10Bitmaps, err := q.EvaluateNumericAttributeValueGreaterOrEqualThan(ctx, store.EvaluateNumericAttributeValueGreaterOrEqualThanParams{
					Name:  "priority",
					Value: store.NumericValueToSQL(10),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(priorityGTE10Bitmaps).To(HaveLen(2))

				priorityCombined := store.NewBitmap()
				for _, bm := range priorityGTE10Bitmaps {
					priorityCombined.Or(bm.Bitmap)
				}

				priorityIDs := priorityCombined.ToArray()
				Expect(priorityIDs).To(HaveLen(2))

				priorityPayloads, err := q.RetrievePayloads(ctx, priorityIDs)
				Expect(err).NotTo(HaveOccurred())
				Expect(priorityPayloads).To(HaveLen(2))

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Update operation", func() {
		It("should update payload and bitmap indexes correctly", func() {
			key := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original content"),
									StringAttributes: map[string]string{
										"status": "draft",
									},
									NumericAttributes: map[string]uint64{
										"version": 1,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Now update the entity
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "application/json",
									BTL:         1000,
									Owner:       owner,
									Content:     []byte(`{"updated": true}`),
									StringAttributes: map[string]string{
										"status": "published",
									},
									NumericAttributes: map[string]uint64{
										"version": 2,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify the update
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())

				Expect(row.Payload).To(Equal([]byte(`{"updated": true}`)))
				Expect(row.ContentType).To(Equal("application/json"))
				Expect(row.StringAttributes.Values["status"]).To(Equal("published"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(2)))
				Expect(row.NumericAttributes.Values["$lastModifiedAtBlock"]).To(Equal(uint64(101)))
				// $createdAtBlock should be preserved
				Expect(row.NumericAttributes.Values["$createdAtBlock"]).To(Equal(uint64(100)))

				// Verify old bitmap index is removed
				oldStatusBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "status",
					Value: "draft",
				})
				Expect(err).To(HaveOccurred()) // Should not find old value

				// Verify new bitmap index exists
				newStatusBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "status",
					Value: "published",
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newStatusBitmap.ToArray()).To(HaveLen(1))

				_ = oldStatusBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Delete operation", func() {
		It("should delete payload and remove bitmap indexes", func() {
			key := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("to be deleted"),
									StringAttributes: map[string]string{
										"deletable": "yes",
									},
									NumericAttributes: map[string]uint64{
										"importance": 5,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity exists
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now delete the entity
			deleteIterator := pusher.NewPushIterator()
			deleteKey := events.OPDelete(key)
			deleteBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Delete:  &deleteKey,
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				deleteIterator.Push(ctx, deleteBatch)
				deleteIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(deleteIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity is deleted
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).To(HaveOccurred())

				// Verify bitmap index is removed
				_, err = q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "deletable",
					Value: "yes",
				})
				Expect(err).To(HaveOccurred())

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents Expire operation", func() {
		It("should expire payload and remove bitmap indexes", func() {
			key := common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         10, // Short BTL
									Owner:       owner,
									Content:     []byte("will expire"),
									StringAttributes: map[string]string{
										"expirable": "yes",
									},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Now expire the entity
			expireIterator := pusher.NewPushIterator()
			expireKey := events.OPExpire(key)
			expireBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 111, // After expiration
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Expire:  &expireKey,
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				expireIterator.Push(ctx, expireBatch)
				expireIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(expireIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify entity is expired (deleted)
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				_, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).To(HaveOccurred())
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents ExtendBTL operation", func() {
		It("should extend expiration and update bitmap indexes", func() {
			key := common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original expiration
			var originalExpiration uint64
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				originalExpiration = row.NumericAttributes.Values["$expiration"]
				Expect(originalExpiration).To(Equal(uint64(600))) // 100 + 500
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now extend BTL
			extendIterator := pusher.NewPushIterator()
			extendBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 200,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								ExtendBTL: &events.OPExtendBTL{
									Key: key,
									BTL: 1000,
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				extendIterator.Push(ctx, extendBatch)
				extendIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(extendIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify extended expiration
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				newExpiration := row.NumericAttributes.Values["$expiration"]
				Expect(newExpiration).To(Equal(uint64(1600)))

				// Verify old expiration bitmap is removed
				oldExpBitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "$expiration",
					Value: store.NumericValueToSQL(600),
				})
				Expect(err).To(HaveOccurred())

				// Verify new expiration bitmap exists
				newExpBitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "$expiration",
					Value: store.NumericValueToSQL(1600),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newExpBitmap.ToArray()).To(HaveLen(1))

				_ = oldExpBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents ChangeOwner operation", func() {
		It("should change owner and update bitmap indexes", func() {
			key := common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
			originalOwner := common.HexToAddress("0x1111111111111111111111111111111111111111")
			newOwner := common.HexToAddress("0x2222222222222222222222222222222222222222")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             originalOwner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original owner
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(originalOwner.Hex())))
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(originalOwner.Hex())))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Now change owner
			changeOwnerIterator := pusher.NewPushIterator()
			changeOwnerBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								ChangeOwner: &events.OPChangeOwner{
									Key:   key,
									Owner: newOwner,
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				changeOwnerIterator.Push(ctx, changeOwnerBatch)
				changeOwnerIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(changeOwnerIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify new owner and creator preserved
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(newOwner.Hex())))
				// $creator should be preserved
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(originalOwner.Hex())))

				// Verify old owner bitmap is removed
				oldOwnerBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "$owner",
					Value: strings.ToLower(originalOwner.Hex()),
				})
				Expect(err).To(HaveOccurred())

				// Verify new owner bitmap exists
				newOwnerBitmap, err := q.EvaluateStringAttributeValueEqual(ctx, store.EvaluateStringAttributeValueEqualParams{
					Name:  "$owner",
					Value: strings.ToLower(newOwner.Hex()),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(newOwnerBitmap.ToArray()).To(HaveLen(1))

				_ = oldOwnerBitmap
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents multiple updates to same key", func() {
		It("should only apply the last update in a block", func() {
			key := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original"),
									StringAttributes: map[string]string{
										"status": "v0",
									},
									NumericAttributes: map[string]uint64{
										"version": 0,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Send a single update (the last one) - this is the only one that will be applied
			// When multiple updates to the same key exist in a block, only the last one is applied
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 1,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("final update"),
									StringAttributes: map[string]string{
										"status": "v3",
									},
									NumericAttributes: map[string]uint64{
										"version": 3,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify the update was applied
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Payload).To(Equal([]byte("final update")))
				Expect(row.StringAttributes.Values["status"]).To(Equal("v3"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(3)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should skip non-last updates and only process the last update for a key", func() {
			key := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0001")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create the entity
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("original"),
									StringAttributes: map[string]string{
										"status": "v0",
									},
									NumericAttributes: map[string]uint64{
										"version": 0,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Send multiple updates in the same block - only the LAST one should be processed
			// The code uses `continue operationLoop` to skip non-last updates and continue to next operation
			updateIterator := pusher.NewPushIterator()
			updateBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 101,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("first update - skipped"),
									StringAttributes: map[string]string{
										"status": "v1",
									},
									NumericAttributes: map[string]uint64{
										"version": 1,
									},
								},
							},
							{
								TxIndex: 0,
								OpIndex: 1,
								Update: &events.OPUpdate{
									Key:         key,
									ContentType: "text/plain",
									BTL:         500,
									Owner:       owner,
									Content:     []byte("second update - last one"),
									StringAttributes: map[string]string{
										"status": "v2",
									},
									NumericAttributes: map[string]uint64{
										"version": 2,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				updateIterator.Push(ctx, updateBatch)
				updateIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(updateIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// With `continue operationLoop`, non-last updates are skipped but processing
			// continues to the next operation. The last update for the key is applied.
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				// The last update (second one) should be applied
				Expect(row.Payload).To(Equal([]byte("second update - last one")))
				Expect(row.StringAttributes.Values["status"]).To(Equal("v2"))
				Expect(row.NumericAttributes.Values["version"]).To(Equal(uint64(2)))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents skip already processed blocks", func() {
		It("should skip blocks that have already been processed", func() {
			key := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			// First create entity at block 100
			createIterator := pusher.NewPushIterator()
			createBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("original"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				createIterator.Push(ctx, createBatch)
				createIterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(createIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Try to replay the same block - should be skipped
			replayIterator := pusher.NewPushIterator()
			replayBatch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100, // Same block number
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("should be ignored"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				replayIterator.Push(ctx, replayBatch)
				replayIterator.Close()
			}()

			err = sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(replayIterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			// Verify original content is preserved
			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())
				Expect(row.Payload).To(Equal([]byte("original")))
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents batch error handling", func() {
		It("should return error when batch contains an error", func() {
			// Create a custom iterator that yields an error
			errorIterator := func(yield func(arkivevents.BatchOrError) bool) {
				yield(arkivevents.BatchOrError{
					Batch: events.BlockBatch{},
					Error: errors.New("simulated batch error"),
				})
			}

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(errorIterator))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("simulated batch error"))
		})
	})

	Describe("FollowEvents system attributes", func() {
		It("should set all system attributes correctly on create", func() {
			key := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			iterator := pusher.NewPushIterator()
			batch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 5,
								OpIndex: 3,
								Create: &events.OPCreate{
									Key:               key,
									ContentType:       "text/plain",
									BTL:               500,
									Owner:             owner,
									Content:           []byte("content"),
									StringAttributes:  map[string]string{},
									NumericAttributes: map[string]uint64{},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				row, err := q.GetPayloadForEntityKey(ctx, key.Bytes())
				Expect(err).NotTo(HaveOccurred())

				// String attributes
				Expect(row.StringAttributes.Values["$owner"]).To(Equal(strings.ToLower(owner.Hex())))
				Expect(row.StringAttributes.Values["$creator"]).To(Equal(strings.ToLower(owner.Hex())))
				Expect(row.StringAttributes.Values["$key"]).To(Equal(strings.ToLower(key.Hex())))

				// Numeric attributes
				Expect(row.NumericAttributes.Values["$expiration"]).To(Equal(uint64(600))) // 100 + 500
				Expect(row.NumericAttributes.Values["$createdAtBlock"]).To(Equal(uint64(100)))
				Expect(row.NumericAttributes.Values["$lastModifiedAtBlock"]).To(Equal(uint64(100)))
				Expect(row.NumericAttributes.Values["$txIndex"]).To(Equal(uint64(5)))
				Expect(row.NumericAttributes.Values["$opIndex"]).To(Equal(uint64(3)))

				// Verify sequence calculation
				expectedSequence := uint64(100)<<32 | uint64(5)<<16 | uint64(3)
				Expect(row.NumericAttributes.Values["$sequence"]).To(Equal(expectedSequence))

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("FollowEvents with numeric attribute values of 2^63 or more", func() {
		It("should index the entity and find it again by equality", func() {
			// Regression test: values with the top bit set used to fail at the
			// database layer ("uint64 values with high bit set are not
			// supported"), which permanently stopped the event follower.
			iterator := pusher.NewPushIterator()

			key := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
			owner := common.HexToAddress("0x1234567890123456789012345678901234567890")

			batch := events.BlockBatch{
				Blocks: []events.Block{
					{
						Number: 100,
						Operations: []events.Operation{
							{
								TxIndex: 0,
								OpIndex: 0,
								Create: &events.OPCreate{
									Key:         key,
									ContentType: "application/json",
									BTL:         1000,
									Owner:       owner,
									Content:     []byte(`{"name": "huge numbers"}`),
									StringAttributes: map[string]string{
										"type": "huge",
									},
									NumericAttributes: map[string]uint64{
										"big":     math.MaxUint64,
										"alsoBig": 1 << 63,
										"small":   7,
									},
								},
							},
						},
					},
				},
			}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()

			err := sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))
			Expect(err).NotTo(HaveOccurred())

			lastBlock, err := sqlStore.GetLastBlock(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(lastBlock).To(Equal(uint64(100)))

			err = sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				for name, value := range map[string]uint64{
					"big":     math.MaxUint64,
					"alsoBig": 1 << 63,
					"small":   7,
				} {
					bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
						Name:  name,
						Value: store.NumericValueToSQL(value),
					})
					Expect(err).NotTo(HaveOccurred(), "equality lookup for %q", name)
					Expect(bitmap).NotTo(BeNil())

					ids := bitmap.ToArray()
					Expect(ids).To(HaveLen(1))

					payloads, err := q.RetrievePayloads(ctx, ids)
					Expect(err).NotTo(HaveOccurred())
					Expect(payloads).To(HaveLen(1))
					Expect(payloads[0].NumericAttributes.Values[name]).To(Equal(value))
				}

				// Inclusion (IN) lookups must round-trip huge values too.
				bitmaps, err := q.EvaluateNumericAttributeValueInclusion(ctx, store.EvaluateNumericAttributeValueInclusionParams{
					Name:   "big",
					Values: store.NumericValuesToSQL([]uint64{math.MaxUint64, 42}),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(bitmaps).To(HaveLen(1))

				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("range queries after the int64 storage change", func() {
		// Helper: run a numeric range eval and return the set of matching
		// payload keys (by their "name" string attribute for readability).
		type rangeCase struct {
			op    string
			bound uint64
		}

		var runRange func(q *store.Queries, attr string, c rangeCase) []uint64

		BeforeEach(func() {
			runRange = func(q *store.Queries, attr string, c rangeCase) []uint64 {
				var (
					bitmaps []*store.Bitmap
					err     error
				)
				params := struct {
					Name  string
					Value int64
				}{attr, store.NumericValueToSQL(c.bound)}

				switch c.op {
				case "<":
					bitmaps, err = q.EvaluateNumericAttributeValueLowerThan(ctx, store.EvaluateNumericAttributeValueLowerThanParams(params))
				case "<=":
					bitmaps, err = q.EvaluateNumericAttributeValueLessOrEqualThan(ctx, store.EvaluateNumericAttributeValueLessOrEqualThanParams(params))
				case ">":
					bitmaps, err = q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams(params))
				case ">=":
					bitmaps, err = q.EvaluateNumericAttributeValueGreaterOrEqualThan(ctx, store.EvaluateNumericAttributeValueGreaterOrEqualThanParams(params))
				}
				Expect(err).NotTo(HaveOccurred())

				combined := store.NewBitmap()
				for _, bm := range bitmaps {
					combined.Or(bm.Bitmap)
				}
				return combined.ToArray()
			}
		})

		follow := func(numericAttrsPerKey map[common.Hash]map[string]uint64) {
			iterator := pusher.NewPushIterator()
			ops := []events.Operation{}
			txIndex := uint64(0)
			for key, attrs := range numericAttrsPerKey {
				ops = append(ops, events.Operation{
					TxIndex: txIndex,
					OpIndex: 0,
					Create: &events.OPCreate{
						Key:               key,
						ContentType:       "application/json",
						BTL:               1000,
						Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
						Content:           []byte(`{}`),
						StringAttributes:  map[string]string{"kind": "range-test"},
						NumericAttributes: attrs,
					},
				})
				txIndex++
			}
			batch := events.BlockBatch{Blocks: []events.Block{{Number: 100, Operations: ops}}}

			go func() {
				defer GinkgoRecover()
				iterator.Push(ctx, batch)
				iterator.Close()
			}()
			Expect(sqlStore.FollowEvents(ctx, arkivevents.BatchIterator(iterator.Iterator()))).To(Succeed())
		}

		It("keeps range queries exact when all values are below 2^63 (all pre-existing data)", func() {
			// Values below 2^63 are stored as the very same number as before
			// this change, so <, <=, >, >= must behave identically.
			follow(map[common.Hash]map[string]uint64{
				common.HexToHash("0x01"): {"priority": 5},
				common.HexToHash("0x02"): {"priority": 100},
				common.HexToHash("0x03"): {"priority": 7000},
			})

			err := sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				Expect(runRange(q, "priority", rangeCase{"<", 50})).To(HaveLen(1))     // {5}
				Expect(runRange(q, "priority", rangeCase{"<=", 100})).To(HaveLen(2))   // {5, 100}
				Expect(runRange(q, "priority", rangeCase{">", 50})).To(HaveLen(2))     // {100, 7000}
				Expect(runRange(q, "priority", rangeCase{">=", 7000})).To(HaveLen(1))  // {7000}
				Expect(runRange(q, "priority", rangeCase{">", 7000})).To(BeEmpty())    // {}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("orders values of 2^63 and above correctly in range queries", func() {
			// The stored form is value XOR 2^63 (as int64), which is strictly
			// increasing over the whole uint64 range, so SQL's signed
			// comparison must agree with numeric comparison even across the
			// 2^63 boundary.
			follow(map[common.Hash]map[string]uint64{
				common.HexToHash("0x0a"): {"size": 100},
				common.HexToHash("0x0b"): {"size": 1 << 63},        // just past the boundary
				common.HexToHash("0x0c"): {"size": math.MaxUint64}, // largest possible
			})

			err := sqlStore.ReadTransaction(ctx, func(q *store.Queries) error {
				Expect(runRange(q, "size", rangeCase{">", 50})).To(HaveLen(3))                    // all of them
				Expect(runRange(q, "size", rangeCase{"<", 50})).To(BeEmpty())                     // none
				Expect(runRange(q, "size", rangeCase{">", 100})).To(HaveLen(2))                   // the two huge ones
				Expect(runRange(q, "size", rangeCase{">=", 1 << 63})).To(HaveLen(2))              // both at/above 2^63
				Expect(runRange(q, "size", rangeCase{">", 1 << 63})).To(HaveLen(1))               // only MaxUint64
				Expect(runRange(q, "size", rangeCase{"<", 1 << 63})).To(HaveLen(1))               // only 100
				Expect(runRange(q, "size", rangeCase{">=", math.MaxUint64})).To(HaveLen(1))       // only MaxUint64
				Expect(runRange(q, "size", rangeCase{"<=", math.MaxUint64})).To(HaveLen(3))       // everything
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("migration 000002 on a database written before the encoding change", func() {
		It("re-encodes existing rows so old data keeps answering queries correctly", func() {
			// Build a database the way released versions wrote it: schema at
			// migration version 1, numeric values stored raw. Opening the
			// store must migrate it and both equality and range queries must
			// keep working on the old rows.
			dbPath := filepath.Join(tmpDir, "legacy.db")

			raw, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=rwc&_journal_mode=WAL")
			Expect(err).NotTo(HaveOccurred())

			initSQL, err := store.Migrations.ReadFile("schema/000001_init.up.sql")
			Expect(err).NotTo(HaveOccurred())
			_, err = raw.Exec(string(initSQL))
			Expect(err).NotTo(HaveOccurred())

			// golang-migrate's version table, pinned at version 1.
			_, err = raw.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version uint64, dirty bool);
				DELETE FROM schema_migrations; INSERT INTO schema_migrations (version, dirty) VALUES (1, 0);`)
			Expect(err).NotTo(HaveOccurred())

			// A row exactly as the pre-fix code stored it: raw value 100.
			legacyBitmap := store.NewBitmap()
			legacyBitmap.Add(7)
			blob, err := legacyBitmap.Value()
			Expect(err).NotTo(HaveOccurred())
			_, err = raw.Exec(`INSERT INTO numeric_attributes_values_bitmaps (name, value, bitmap) VALUES (?, ?, ?)`,
				"legacy", int64(100), blob)
			Expect(err).NotTo(HaveOccurred())
			Expect(raw.Close()).To(Succeed())

			// Opening the store runs migration 000002.
			legacyStore, err := sqlitebitmapstore.NewSQLiteStore(logger, dbPath, 2)
			Expect(err).NotTo(HaveOccurred())
			defer legacyStore.Close()

			err = legacyStore.ReadTransaction(ctx, func(q *store.Queries) error {
				bitmap, err := q.EvaluateNumericAttributeValueEqual(ctx, store.EvaluateNumericAttributeValueEqualParams{
					Name:  "legacy",
					Value: store.NumericValueToSQL(100),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(bitmap).NotTo(BeNil())
				Expect(bitmap.ToArray()).To(Equal([]uint64{7}))

				greater, err := q.EvaluateNumericAttributeValueGreaterThan(ctx, store.EvaluateNumericAttributeValueGreaterThanParams{
					Name:  "legacy",
					Value: store.NumericValueToSQL(50),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(greater).To(HaveLen(1))

				lower, err := q.EvaluateNumericAttributeValueLowerThan(ctx, store.EvaluateNumericAttributeValueLowerThanParams{
					Name:  "legacy",
					Value: store.NumericValueToSQL(50),
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(lower).To(BeEmpty())
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
