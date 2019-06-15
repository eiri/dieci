package dieci

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestScoreMakeScore to ensure we can generate score
func TestScoreMakeScore(t *testing.T) {
	data := []byte("brown fox")
	score := MakeScore(data)
	expect := "7113fd84e8973eb2"
	assert.Equal(t, expect, fmt.Sprintf("%s", score))
}
