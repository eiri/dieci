package dieci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestScore to ensure we can generate score
func TestScore(t *testing.T) {
	data := []byte("brown fox")
	score1 := NewScore(data)
	expectString := "7113fd84e8973eb2"
	assert.Equal(t, expectString, score1.String())

	score2 := NewScore(data)
	assert.Equal(t, expectString, score2.String())
}
