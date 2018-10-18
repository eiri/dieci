package beansdb_test

import (
	"github.com/eiri/beansdb"
	"testing"
)

// TestNew to ensure we can create new storage
func TestNew(t *testing.T) {
	beansdb.Open()
}
