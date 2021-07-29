package dieci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestScore to ensure we can generate score
func TestScore(t *testing.T) {
	data := []byte("brown fox")
	score1 := newScore(data)
	expectString := "7113fd84e8973eb2"
	expectUint64 := uint64(8148134898123095730)
	assert.Equal(t, expectString, score1.String())
	assert.Equal(t, expectUint64, score1.uint64())

	score2 := newScore(data)
	assert.Equal(t, expectString, score2.String())
	assert.Equal(t, expectUint64, score2.uint64())
}
