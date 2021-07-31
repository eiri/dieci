package dieci

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/cespare/xxhash"
)

// scoreSize is the size of score in bytes
const scoreSize = 8

// Score is a type alias for score representation
type Score []byte

// NewScore returns a new score for a given data
func NewScore(data []byte) Score {
	h := xxhash.Sum64(data)
	sc := make([]byte, scoreSize)
	binary.BigEndian.PutUint64(sc, h)
	return Score(sc)
}

// String added to comply with Stringer interface
func (s Score) String() string {
	return hex.EncodeToString(s)
}

// Uint64 returns original xxhash sum64 for a given score
func (s Score) Uint64() uint64 {
	h := binary.BigEndian.Uint64(s)
	return h
}
