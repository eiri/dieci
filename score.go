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

func newScore(data []byte) score {
	h := xxhash.Sum64(data)
	sc := make([]byte, scoreSize)
	binary.BigEndian.PutUint64(sc, h)
	return score(sc)
}

// String added to comply with Stringer interface
func (s score) String() string {
	return hex.EncodeToString(s)
}

// uint64 returns original xxhash sum64 for a given score
func (s score) uint64() uint64 {
	h := binary.BigEndian.Uint64(s)
	return h
}
