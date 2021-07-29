package dieci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestKey to ensure we can generate keys
func TestKey(t *testing.T) {
	key0 := newKey()
	for i := 0; i < 10; i++ {
		key1 := newKey()
		key2 := newKey()

		assert.NotEqual(t, key0, key1)
		assert.Greater(t, key1.String(), key0.String())

		assert.NotEqual(t, key2, key1)
		assert.Greater(t, key2.String(), key1.String())

		time.Sleep(100 * time.Millisecond)
		key0 = key2
	}
}
