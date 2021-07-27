package dieci

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/cespare/xxhash"
)

// scoreSize is the size of score in bytes
const scoreSize = 8

// score is a type alias for score representation
type score []byte

// String added to comply with Stringer interface
func (s score) String() string {
	return hex.EncodeToString(s)
}

// toUint64 returns original xxhash sum64 for a given score
func (s score) toUint64() uint64 {
	h := binary.BigEndian.Uint64(s)
	return h
}

// makeScore creates a score for a given data block
func makeScore(b []byte) score {
	h := xxhash.Sum64(b)
	s := make([]byte, scoreSize)
	binary.BigEndian.PutUint64(s, h)
	return score(s)
}
