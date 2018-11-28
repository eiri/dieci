// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/md5"
	"encoding/hex"
)

// ScoreSize is the size of score in bytes
const ScoreSize = 16

// Score is type alias for score representation
type Score [ScoreSize]byte

func (s Score) String() string {
	return hex.EncodeToString(s[:])
}

// MakeScore creates a score for a given data block
func MakeScore(b []byte) Score {
	score := md5.Sum(b)
	return score
}
