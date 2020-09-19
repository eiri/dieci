package dieci

import (
	"strconv"

	"github.com/OneOfOne/xxhash"
)

// scoreSize is the size of score in bytes
const scoreSize = 8

// Score is type alias for score representation
// type Score [scoreSize]byte
type Score uint64

func (s Score) String() string {
	return strconv.FormatInt(int64(s), 16)
}

// MakeScore creates a score for a given data block
func MakeScore(b []byte) Score {
	sum := xxhash.Checksum64(b)
	return Score(sum)
}
