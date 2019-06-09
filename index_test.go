package dieci

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex for compliance to Indexer
func TestIndex(t *testing.T) {
	assert := require.New(t)

	words := "The quick brown fox jumps over the lazy dog"
	var index []byte

	t.Run("write", func(t *testing.T) {
		idxRW := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(idxRW)
		assert.NoError(err)
		assert.Equal(0, idx.Cur())
		pos := 0
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			expAddr := Addr{pos, size}
			err := idx.Write(score, size)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score])
			pos += size
			err = idx.Write(score, 0)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score], "Should ignore update")
		}
		index = make([]byte, idxRW.Len())
		copy(index, idxRW.Bytes())
	})

	t.Run("open", func(t *testing.T) {
		tmp := make([]byte, len(index))
		copy(tmp, index)
		idxRW := bytes.NewBuffer(tmp)
		idx, err := NewIndex(idxRW)
		assert.NoError(err)
		assert.Len(idx.cache, len(strings.Fields(words)))
		assert.Equal(35, idx.Cur())
	})

	t.Run("read", func(t *testing.T) {
		tmp := make([]byte, len(index))
		copy(tmp, index)
		idxRW := bytes.NewBuffer(tmp)
		idx, err := NewIndex(idxRW)
		assert.NoError(err)
		assert.Equal(35, idx.Cur())
		pos := 0
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			a, ok := idx.Read(score)
			assert.Equal(pos, a.pos, "Should return correct position")
			assert.Equal(size, a.size, "Should return correct size")
			assert.True(ok, "Should indicate that score exists")
			pos += size
		}
		score := MakeScore([]byte("missing"))
		a, ok := idx.Read(score)
		assert.Empty(a.pos, "Should return 0 position for missing score")
		assert.Empty(a.size, "Should return 0 size for missing score")
		assert.False(ok, "Should indicate that score doesn't exists")
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
