package dieci

import (
	"crypto/md5"
	"encoding/hex"
)

// scoreSize is the size of score in bytes
const scoreSize = 16

// Score is type alias for score representation
type Score [scoreSize]byte

func (s Score) String() string {
	return hex.EncodeToString(s[:])
}

// MakeScore creates a score for a given data block
func MakeScore(b []byte) Score {
	score := md5.Sum(b)
	return score
}
