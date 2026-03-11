package pebblestore

import "encoding/binary"

// Key prefix constants for PebbleDB key namespacing.
const (
	prefixLastBlock      byte = 0x01
	prefixPayload        byte = 0x02
	prefixEntityCurrent  byte = 0x03
	prefixFromBlockIndex byte = 0x04
	prefixToBlockIndex   byte = 0x05
	prefixIDCounter      byte = 0x06
	prefixStringBitmap   byte = 0x10
	prefixNumericBitmap  byte = 0x20
)

// lastBlockKey returns the key for the last-block record.
// Layout: [0x01]
func lastBlockKey() []byte {
	return []byte{prefixLastBlock}
}

// idCounterKey returns the key for the ID counter record.
// Layout: [0x06]
func idCounterKey() []byte {
	return []byte{prefixIDCounter}
}

// payloadKey returns the key for a payload record identified by id.
// Layout: [0x02][id:8BE]
func payloadKey(id uint64) []byte {
	key := make([]byte, 1+8)
	key[0] = prefixPayload
	binary.BigEndian.PutUint64(key[1:], id)
	return key
}

// entityCurrentKey returns the key that maps a 32-byte entity key to its
// current payload ID.
// Layout: [0x03][entityKey:32]
func entityCurrentKey(entityKey []byte) []byte {
	key := make([]byte, 1+32)
	key[0] = prefixEntityCurrent
	copy(key[1:], entityKey[:32])
	return key
}

// fromBlockIndexKey returns a temporal index key for looking up payloads
// by their from_block value.
// Layout: [0x04][fromBlock:8BE][id:8BE]
func fromBlockIndexKey(fromBlock, id uint64) []byte {
	key := make([]byte, 1+8+8)
	key[0] = prefixFromBlockIndex
	binary.BigEndian.PutUint64(key[1:], fromBlock)
	binary.BigEndian.PutUint64(key[9:], id)
	return key
}

// toBlockIndexKey returns a pruning index key for looking up payloads
// by their to_block value.
// Layout: [0x05][toBlock:8BE][id:8BE]
func toBlockIndexKey(toBlock, id uint64) []byte {
	key := make([]byte, 1+8+8)
	key[0] = prefixToBlockIndex
	binary.BigEndian.PutUint64(key[1:], toBlock)
	binary.BigEndian.PutUint64(key[9:], id)
	return key
}

// stringBitmapKey returns the key for a string-typed bitmap entry.
// Layout: [0x10][nameLen:2BE][name][valueLen:2BE][value][block:8BE]
func stringBitmapKey(name, value string, block uint64) []byte {
	nameBytes := []byte(name)
	valueBytes := []byte(value)
	key := make([]byte, 1+2+len(nameBytes)+2+len(valueBytes)+8)
	off := 0
	key[off] = prefixStringBitmap
	off++
	binary.BigEndian.PutUint16(key[off:], uint16(len(nameBytes)))
	off += 2
	copy(key[off:], nameBytes)
	off += len(nameBytes)
	binary.BigEndian.PutUint16(key[off:], uint16(len(valueBytes)))
	off += 2
	copy(key[off:], valueBytes)
	off += len(valueBytes)
	binary.BigEndian.PutUint64(key[off:], block)
	return key
}

// stringBitmapPrefix returns a prefix for scanning all values of a given
// string bitmap name.
// Layout: [0x10][nameLen:2BE][name]
func stringBitmapPrefix(name string) []byte {
	nameBytes := []byte(name)
	prefix := make([]byte, 1+2+len(nameBytes))
	off := 0
	prefix[off] = prefixStringBitmap
	off++
	binary.BigEndian.PutUint16(prefix[off:], uint16(len(nameBytes)))
	off += 2
	copy(prefix[off:], nameBytes)
	return prefix
}

// stringBitmapNameValuePrefix returns a prefix for scanning all blocks of a
// specific (name, value) pair in the string bitmap index.
// Layout: [0x10][nameLen:2BE][name][valueLen:2BE][value]
func stringBitmapNameValuePrefix(name, value string) []byte {
	nameBytes := []byte(name)
	valueBytes := []byte(value)
	prefix := make([]byte, 1+2+len(nameBytes)+2+len(valueBytes))
	off := 0
	prefix[off] = prefixStringBitmap
	off++
	binary.BigEndian.PutUint16(prefix[off:], uint16(len(nameBytes)))
	off += 2
	copy(prefix[off:], nameBytes)
	off += len(nameBytes)
	binary.BigEndian.PutUint16(prefix[off:], uint16(len(valueBytes)))
	off += 2
	copy(prefix[off:], valueBytes)
	return prefix
}

// numericBitmapKey returns the key for a numeric-typed bitmap entry.
// Layout: [0x20][nameLen:2BE][name][value:8BE][block:8BE]
func numericBitmapKey(name string, value, block uint64) []byte {
	nameBytes := []byte(name)
	key := make([]byte, 1+2+len(nameBytes)+8+8)
	off := 0
	key[off] = prefixNumericBitmap
	off++
	binary.BigEndian.PutUint16(key[off:], uint16(len(nameBytes)))
	off += 2
	copy(key[off:], nameBytes)
	off += len(nameBytes)
	binary.BigEndian.PutUint64(key[off:], value)
	off += 8
	binary.BigEndian.PutUint64(key[off:], block)
	return key
}

// numericBitmapPrefix returns a prefix for scanning all values of a given
// numeric bitmap name.
// Layout: [0x20][nameLen:2BE][name]
func numericBitmapPrefix(name string) []byte {
	nameBytes := []byte(name)
	prefix := make([]byte, 1+2+len(nameBytes))
	off := 0
	prefix[off] = prefixNumericBitmap
	off++
	binary.BigEndian.PutUint16(prefix[off:], uint16(len(nameBytes)))
	off += 2
	copy(prefix[off:], nameBytes)
	return prefix
}

// numericBitmapNameValuePrefix returns a prefix for scanning all blocks of a
// specific (name, value) pair in the numeric bitmap index.
// Layout: [0x20][nameLen:2BE][name][value:8BE]
func numericBitmapNameValuePrefix(name string, value uint64) []byte {
	nameBytes := []byte(name)
	prefix := make([]byte, 1+2+len(nameBytes)+8)
	off := 0
	prefix[off] = prefixNumericBitmap
	off++
	binary.BigEndian.PutUint16(prefix[off:], uint16(len(nameBytes)))
	off += 2
	copy(prefix[off:], nameBytes)
	off += len(nameBytes)
	binary.BigEndian.PutUint64(prefix[off:], value)
	return prefix
}

// --- Key parser functions ---

// parsePayloadKey extracts the payload id from a payload key.
// Expects layout: [0x02][id:8BE]
func parsePayloadKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[1:])
}

// parseFromBlockIndexKey extracts the fromBlock and id from a from-block
// index key.
// Expects layout: [0x04][fromBlock:8BE][id:8BE]
func parseFromBlockIndexKey(key []byte) (fromBlock, id uint64) {
	fromBlock = binary.BigEndian.Uint64(key[1:])
	id = binary.BigEndian.Uint64(key[9:])
	return
}

// parseToBlockIndexKey extracts the toBlock and id from a to-block index key.
// Expects layout: [0x05][toBlock:8BE][id:8BE]
func parseToBlockIndexKey(key []byte) (toBlock, id uint64) {
	toBlock = binary.BigEndian.Uint64(key[1:])
	id = binary.BigEndian.Uint64(key[9:])
	return
}

// parseStringBitmapKey extracts the name, value, and block number from a
// string bitmap key.
// Expects layout: [0x10][nameLen:2BE][name][valueLen:2BE][value][block:8BE]
func parseStringBitmapKey(key []byte) (name, value string, block uint64) {
	off := 1 // skip prefix
	nameLen := int(binary.BigEndian.Uint16(key[off:]))
	off += 2
	name = string(key[off : off+nameLen])
	off += nameLen
	valueLen := int(binary.BigEndian.Uint16(key[off:]))
	off += 2
	value = string(key[off : off+valueLen])
	off += valueLen
	block = binary.BigEndian.Uint64(key[off:])
	return
}

// parseNumericBitmapKey extracts the name, value, and block number from a
// numeric bitmap key.
// Expects layout: [0x20][nameLen:2BE][name][value:8BE][block:8BE]
func parseNumericBitmapKey(key []byte) (name string, value, block uint64) {
	off := 1 // skip prefix
	nameLen := int(binary.BigEndian.Uint16(key[off:]))
	off += 2
	name = string(key[off : off+nameLen])
	off += nameLen
	value = binary.BigEndian.Uint64(key[off:])
	off += 8
	block = binary.BigEndian.Uint64(key[off:])
	return
}

// --- Prefix scan helper ---

// prefixUpperBound returns the upper bound key for a prefix scan. It
// increments the last byte of the prefix. If the last byte is 0xFF, it is
// removed and the next-to-last byte is incremented, repeating as needed.
// Returns nil if all bytes are 0xFF (the prefix covers the entire keyspace).
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for len(upper) > 0 {
		last := len(upper) - 1
		if upper[last] < 0xFF {
			upper[last]++
			return upper
		}
		// Last byte is 0xFF; drop it and try to increment the previous byte.
		upper = upper[:last]
	}
	return nil
}
