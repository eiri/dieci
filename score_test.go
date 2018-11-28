package beansdb_test

import (
	"fmt"
	"github.com/eiri/beansdb"
	"testing"
)

// TestScoreMakeScore to ensure we can generate score
func TestScoreMakeScore(t *testing.T) {
	data := []byte("brown fox")
	score := beansdb.MakeScore(data)
	expect := "fdd929ffb0a167ab33e8b1a8905858cf"
	if fmt.Sprintf("%s", score) != expect {
		t.Fatalf("Expecting score %q, got %q", expect, score)
	}
}
