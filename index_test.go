package dieci

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex for compliance to Indexer
func TestIndex(t *testing.T) {
	assert := require.New(t)

	var idxtests = []struct {
		in   string
		addr Addr
		ok   bool
	}{
		{"the", Addr{pos: 0, size: 3}, true},
		{"quick", Addr{pos: 3, size: 5}, true},
		{"brown", Addr{pos: 8, size: 5}, true},
		{"fox", Addr{pos: 13, size: 3}, true},
		{"jumps", Addr{pos: 16, size: 5}, true},
		{"over", Addr{pos: 21, size: 4}, true},
		{"missing", Addr{pos: 0, size: 0}, false},
		{"the", Addr{pos: 0, size: 3}, true},
		{"lazy", Addr{pos: 25, size: 4}, true},
		{"dog", Addr{pos: 29, size: 3}, true},
	}

	var index Index

	t.Run("NewIndex", func(t *testing.T) {
		idx := NewIndex()
		assert.Len(idx.cache, 0)
		assert.Equal(0, idx.cur)
	})

	t.Run("Put", func(t *testing.T) {
		idx := NewIndex()
		for _, tt := range idxtests {
			if !tt.ok {
				continue
			}
			data := []byte(tt.in)
			size := len(data)
			score := MakeScore(data)
			before := idx.Len()
			idx.Put(score, size)
			assert.GreaterOrEqual(idx.Len(), before)
			before = idx.Len()
			idx.Put(score, 0)
			assert.Equal(idx.Len(), before, "Should ignore same update")
		}
		index = *idx
	})

	t.Run("Get", func(t *testing.T) {
		for _, tt := range idxtests {
			score := MakeScore([]byte(tt.in))
			addr, ok := index.Get(score)
			if tt.ok {
				assert.True(ok, "Should indicate that score exists")
			} else {
				assert.False(ok, "Should indicate that score is missing")
			}
			assert.Equal(tt.addr, addr, "Should return correct address")
		}
	})
}

// BenchmarkIndexLoad for iterative improvement of index load
func BenchmarkIndexLoad(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		f, err := os.Open("testdata/words.data")
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		idx := NewIndex()
		err = idx.Load(f)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	}
}
