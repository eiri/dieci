package dieci

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex for compliance to Indexer
func TestIndex(t *testing.T) {
	assert := require.New(t)
	name := RandomName()
	err := CreateDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"
	var index []byte

	t.Run("write", func(t *testing.T) {
		idxRW := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(idxRW)
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			expAddr := Addr{pos, size}
			err := idx.Write(score, Addr{pos: pos, size: size})
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score])
			err = idx.Write(score, Addr{pos: 0, size: 0})
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
	})

	t.Run("read", func(t *testing.T) {
		tmp := make([]byte, len(index))
		copy(tmp, index)
		idxRW := bytes.NewBuffer(tmp)
		idx, err := NewIndex(idxRW)
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			a, ok := idx.Read(score)
			assert.Equal(pos, a.pos, "Should return correct position")
			assert.Equal(size, a.size, "Should return correct size")
			assert.True(ok, "Should indicate that score exists")
		}
		score := MakeScore([]byte("missing"))
		a, ok := idx.Read(score)
		assert.Empty(a.pos, "Should return 0 position for missing score")
		assert.Empty(a.size, "Should return 0 size for missing score")
		assert.False(ok, "Should indicate that score doesn't exists")
	})

	// cleanup
	err = removeDatalogFile(name)
	assert.NoError(err)
}

// BenchmarkIndexOpen for iterative improvement of open
func BenchmarkIndexOpen(b *testing.B) {
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
