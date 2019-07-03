package dieci

import (
	"bytes"
	"fmt"
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

	var index []byte

	t.Run("open empty", func(t *testing.T) {
		rw := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(rw)
		assert.NoError(err)
		assert.Len(idx.cache, 0)
		assert.Equal(0, idx.cur)
	})

	t.Run("write", func(t *testing.T) {
		rw := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(rw)
		assert.NoError(err)
		for _, tt := range idxtests {
			if !tt.ok {
				continue
			}
			data := []byte(tt.in)
			size := len(data)
			score := MakeScore(data)
			err := idx.Write(score, size)
			assert.NoError(err)
			before := idx.Len()
			err = idx.Write(score, 0)
			assert.NoError(err)
			assert.Equal(before, idx.Len(), "Should ignore same update")
		}
		index = make([]byte, rw.Len())
		copy(index, rw.Bytes())
	})

	t.Run("open", func(t *testing.T) {
		rw := bytes.NewBuffer(index)
		idx, err := NewIndex(rw)
		assert.NoError(err)
		assert.Len(idx.cache, 8)
		assert.Equal(32, idx.cur)
	})

	t.Run("read", func(t *testing.T) {
		rw := bytes.NewBuffer(index)
		idx, err := NewIndex(rw)
		assert.NoError(err)
		for _, tt := range idxtests {
			score := MakeScore([]byte(tt.in))
			addr, ok := idx.Read(score)
			if tt.ok {
				assert.True(ok, "Should indicate that score exists")
			} else {
				assert.False(ok, "Should indicate that score is missing")
			}
			assert.Equal(tt.addr, addr, "Should return correct address")
		}
	})
}

// BenchmarkOpenIndex for iterative improvement of open
func BenchmarkOpenIndex(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		f, err := os.Open("testdata/words.idx")
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = NewIndex(f)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	}
}

// BenchmarkRebuildIndex for iterative improvement of rebuild
func BenchmarkRebuildIndex(b *testing.B) {
	b.StopTimer()
	// open data file
	name := "testdata/words"
	reader, err := os.Open(name + ".data")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		// create an empty index and set it to datalog
		idxName := fmt.Sprintf("index%05d.idx", n)
		f, err := os.Create(idxName)
		if err != nil {
			b.Fatal(err)
		}
		idx, err := NewIndex(f)
		if err != nil {
			b.Fatal(err)
		}
		// isolated test
		b.StartTimer()
		err = idx.Rebuild(reader)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if idx.Len() != 235886 {
			b.Fatal("expected index cache to be fully propagated")
		}
		f.Close()
		os.Remove(idxName)
	}
	reader.Close()
}
