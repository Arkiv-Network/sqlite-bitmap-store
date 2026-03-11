package pebblestore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"

	"github.com/Arkiv-Network/sqlite-bitmap-store/store"
)

// InsertPayloadParams holds the parameters for inserting a new payload.
type InsertPayloadParams struct {
	EntityKey         []byte
	Payload           []byte
	ContentType       string
	StringAttributes  *store.StringAttributes
	NumericAttributes *store.NumericAttributes
	FromBlock         uint64
}

// GetCurrentPayloadForEntityKeyRow represents the result of looking up the
// current (active) payload for a given entity key.
type GetCurrentPayloadForEntityKeyRow struct {
	EntityKey         []byte
	ID                uint64
	Payload           []byte
	ContentType       string
	StringAttributes  *store.StringAttributes
	NumericAttributes *store.NumericAttributes
	FromBlock         uint64
}

// RetrievePayloadsRow represents a single row returned by RetrievePayloads.
type RetrievePayloadsRow struct {
	EntityKey         []byte
	ID                uint64
	Payload           []byte
	ContentType       string
	StringAttributes  *store.StringAttributes
	NumericAttributes *store.NumericAttributes
}

// encodePayloadValue serializes all payload fields into a single byte slice
// suitable for storage under a 0x02 key.
//
// Layout:
//
//	[fromBlock:8BE][toBlock:8BE][toBlockIsNull:1byte][entityKey:32]
//	[contentTypeLen:2BE][contentType bytes]
//	[stringAttrsLen:4BE][stringAttrsJSON bytes]
//	[numericAttrsLen:4BE][numericAttrsJSON bytes]
//	[payloadBytes...]
func encodePayloadValue(
	fromBlock, toBlock uint64,
	toBlockIsNull bool,
	entityKey []byte,
	contentType string,
	stringAttrs *store.StringAttributes,
	numericAttrs *store.NumericAttributes,
	payload []byte,
) ([]byte, error) {
	stringAttrsJSON, err := json.Marshal(stringAttrs)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: marshal string attributes: %w", err)
	}

	numericAttrsJSON, err := json.Marshal(numericAttrs)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: marshal numeric attributes: %w", err)
	}

	contentTypeBytes := []byte(contentType)

	// Calculate total size.
	size := 8 + // fromBlock
		8 + // toBlock
		1 + // toBlockIsNull
		32 + // entityKey
		2 + len(contentTypeBytes) + // contentTypeLen + contentType
		4 + len(stringAttrsJSON) + // stringAttrsLen + stringAttrsJSON
		4 + len(numericAttrsJSON) + // numericAttrsLen + numericAttrsJSON
		len(payload) // payload bytes (remainder)

	buf := make([]byte, size)
	off := 0

	binary.BigEndian.PutUint64(buf[off:], fromBlock)
	off += 8

	binary.BigEndian.PutUint64(buf[off:], toBlock)
	off += 8

	if toBlockIsNull {
		buf[off] = 0x01
	} else {
		buf[off] = 0x00
	}
	off++

	copy(buf[off:], entityKey[:32])
	off += 32

	binary.BigEndian.PutUint16(buf[off:], uint16(len(contentTypeBytes)))
	off += 2
	copy(buf[off:], contentTypeBytes)
	off += len(contentTypeBytes)

	binary.BigEndian.PutUint32(buf[off:], uint32(len(stringAttrsJSON)))
	off += 4
	copy(buf[off:], stringAttrsJSON)
	off += len(stringAttrsJSON)

	binary.BigEndian.PutUint32(buf[off:], uint32(len(numericAttrsJSON)))
	off += 4
	copy(buf[off:], numericAttrsJSON)
	off += len(numericAttrsJSON)

	copy(buf[off:], payload)

	return buf, nil
}

// decodedPayload holds the fields extracted from a 0x02 payload value.
type decodedPayload struct {
	FromBlock         uint64
	ToBlock           uint64
	ToBlockIsNull     bool
	EntityKey         []byte
	ContentType       string
	StringAttributes  *store.StringAttributes
	NumericAttributes *store.NumericAttributes
	Payload           []byte
}

// decodePayloadValue parses a byte slice written by encodePayloadValue back
// into its component fields.
func decodePayloadValue(data []byte) (decodedPayload, error) {
	var d decodedPayload

	if len(data) < 8+8+1+32+2 {
		return d, fmt.Errorf("pebblestore: payload value too short: %d bytes", len(data))
	}

	off := 0

	d.FromBlock = binary.BigEndian.Uint64(data[off:])
	off += 8

	d.ToBlock = binary.BigEndian.Uint64(data[off:])
	off += 8

	d.ToBlockIsNull = data[off] == 0x01
	off++

	d.EntityKey = make([]byte, 32)
	copy(d.EntityKey, data[off:off+32])
	off += 32

	// contentType
	if off+2 > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at content type length")
	}
	contentTypeLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	if off+contentTypeLen > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at content type")
	}
	d.ContentType = string(data[off : off+contentTypeLen])
	off += contentTypeLen

	// stringAttrs
	if off+4 > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at string attrs length")
	}
	stringAttrsLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	if off+stringAttrsLen > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at string attrs")
	}
	d.StringAttributes = &store.StringAttributes{}
	if err := json.Unmarshal(data[off:off+stringAttrsLen], d.StringAttributes); err != nil {
		return d, fmt.Errorf("pebblestore: unmarshal string attributes: %w", err)
	}
	off += stringAttrsLen

	// numericAttrs
	if off+4 > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at numeric attrs length")
	}
	numericAttrsLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	if off+numericAttrsLen > len(data) {
		return d, fmt.Errorf("pebblestore: payload value truncated at numeric attrs")
	}
	d.NumericAttributes = &store.NumericAttributes{}
	if err := json.Unmarshal(data[off:off+numericAttrsLen], d.NumericAttributes); err != nil {
		return d, fmt.Errorf("pebblestore: unmarshal numeric attributes: %w", err)
	}
	off += numericAttrsLen

	// Remaining bytes are the payload.
	d.Payload = make([]byte, len(data)-off)
	copy(d.Payload, data[off:])

	return d, nil
}

// InsertPayload creates a new payload record, writing the payload data, the
// entity-current pointer, and the from-block index entry into the provided
// batch. It returns the newly allocated ID.
func (s *PebbleStore) InsertPayload(batch *pebble.Batch, arg InsertPayloadParams) (uint64, error) {
	id, err := s.nextID(batch)
	if err != nil {
		return 0, err
	}

	// Encode the payload value with toBlock=0 and toBlockIsNull=true (active).
	val, err := encodePayloadValue(
		arg.FromBlock,
		0,    // toBlock
		true, // toBlockIsNull — payload is active
		arg.EntityKey,
		arg.ContentType,
		arg.StringAttributes,
		arg.NumericAttributes,
		arg.Payload,
	)
	if err != nil {
		return 0, err
	}

	// Write 0x02 payload record.
	if err := batch.Set(payloadKey(id), val, pebble.Sync); err != nil {
		return 0, fmt.Errorf("pebblestore: set payload key: %w", err)
	}

	// Write 0x03 entity-current pointer (value is the 8-byte big-endian ID).
	var idBuf [8]byte
	binary.BigEndian.PutUint64(idBuf[:], id)
	if err := batch.Set(entityCurrentKey(arg.EntityKey), idBuf[:], pebble.Sync); err != nil {
		return 0, fmt.Errorf("pebblestore: set entity current key: %w", err)
	}

	// Write 0x04 from-block index. Value: [toBlock=0:8BE][toBlockIsNull=0x01:1byte].
	var fromIdxVal [9]byte
	// toBlock = 0 is already zero-valued.
	fromIdxVal[8] = 0x01 // toBlockIsNull = true
	if err := batch.Set(fromBlockIndexKey(arg.FromBlock, id), fromIdxVal[:], pebble.Sync); err != nil {
		return 0, fmt.Errorf("pebblestore: set from-block index key: %w", err)
	}

	return id, nil
}

// GetCurrentPayloadForEntityKey reads the current (active) payload for the
// given 32-byte entity key. It returns pebble.ErrNotFound if there is no
// current payload for this entity.
func (s *PebbleStore) GetCurrentPayloadForEntityKey(reader pebble.Reader, entityKey []byte) (GetCurrentPayloadForEntityKeyRow, error) {
	var row GetCurrentPayloadForEntityKeyRow

	// Read 0x03 key to get the current payload ID.
	idVal, closer, err := reader.Get(entityCurrentKey(entityKey))
	if err != nil {
		return row, err
	}
	id := binary.BigEndian.Uint64(idVal)
	closer.Close()

	// Read 0x02 key to get the full payload value.
	payloadVal, closer, err := reader.Get(payloadKey(id))
	if err != nil {
		return row, fmt.Errorf("pebblestore: get payload for id %d: %w", id, err)
	}
	data := make([]byte, len(payloadVal))
	copy(data, payloadVal)
	closer.Close()

	d, err := decodePayloadValue(data)
	if err != nil {
		return row, err
	}

	row.EntityKey = d.EntityKey
	row.ID = id
	row.Payload = d.Payload
	row.ContentType = d.ContentType
	row.StringAttributes = d.StringAttributes
	row.NumericAttributes = d.NumericAttributes
	row.FromBlock = d.FromBlock

	return row, nil
}

// ClosePayloadVersion marks the current payload version for the given entity
// key as closed at the specified block. It updates the payload record, removes
// the entity-current pointer, writes the to-block index entry, and updates
// the from-block index value. If there is no current version for the entity,
// ClosePayloadVersion returns nil.
func (s *PebbleStore) ClosePayloadVersion(batch *pebble.Batch, reader pebble.Reader, entityKey []byte, block uint64) error {
	// Read 0x03 key to get the current payload ID.
	ecKey := entityCurrentKey(entityKey)
	idVal, closer, err := reader.Get(ecKey)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("pebblestore: get entity current: %w", err)
	}
	id := binary.BigEndian.Uint64(idVal)
	closer.Close()

	// Read 0x02 key to get the full payload value so we can decode fromBlock.
	pKey := payloadKey(id)
	payloadVal, closer, err := reader.Get(pKey)
	if err != nil {
		return fmt.Errorf("pebblestore: get payload for close: %w", err)
	}
	data := make([]byte, len(payloadVal))
	copy(data, payloadVal)
	closer.Close()

	d, err := decodePayloadValue(data)
	if err != nil {
		return err
	}

	// Re-encode with toBlock = block, toBlockIsNull = false (closed).
	updatedVal, err := encodePayloadValue(
		d.FromBlock,
		block, // toBlock
		false, // toBlockIsNull = false (closed)
		d.EntityKey,
		d.ContentType,
		d.StringAttributes,
		d.NumericAttributes,
		d.Payload,
	)
	if err != nil {
		return err
	}

	// Update 0x02 payload record.
	if err := batch.Set(pKey, updatedVal, pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: update payload key: %w", err)
	}

	// Delete 0x03 entity-current pointer.
	if err := batch.Delete(ecKey, pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: delete entity current key: %w", err)
	}

	// Write 0x05 to-block index key.
	if err := batch.Set(toBlockIndexKey(block, id), nil, pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: set to-block index key: %w", err)
	}

	// Update 0x04 from-block index value: [toBlock:8BE][toBlockIsNull=0x00:1byte].
	var fromIdxVal [9]byte
	binary.BigEndian.PutUint64(fromIdxVal[:], block)
	fromIdxVal[8] = 0x00 // closed
	if err := batch.Set(fromBlockIndexKey(d.FromBlock, id), fromIdxVal[:], pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: update from-block index key: %w", err)
	}

	return nil
}

// RetrievePayloads fetches the payload records for the given IDs and returns
// them sorted by ID descending.
func (s *PebbleStore) RetrievePayloads(reader pebble.Reader, ids []uint64) ([]RetrievePayloadsRow, error) {
	rows := make([]RetrievePayloadsRow, 0, len(ids))

	for _, id := range ids {
		val, closer, err := reader.Get(payloadKey(id))
		if err == pebble.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("pebblestore: get payload id %d: %w", id, err)
		}
		data := make([]byte, len(val))
		copy(data, val)
		closer.Close()

		d, err := decodePayloadValue(data)
		if err != nil {
			return nil, err
		}

		rows = append(rows, RetrievePayloadsRow{
			EntityKey:         d.EntityKey,
			ID:                id,
			Payload:           d.Payload,
			ContentType:       d.ContentType,
			StringAttributes:  d.StringAttributes,
			NumericAttributes: d.NumericAttributes,
		})
	}

	// Sort by ID descending.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID > rows[j].ID
	})

	return rows, nil
}
