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
	expect := "fdd929ffb0a167ab33e8b1a8905858cf"
	assert.Equal(t, expect, fmt.Sprintf("%s", score))
}
