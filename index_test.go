package dieci

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex for compliance to Indexer
func TestIndex(t *testing.T) {
	assert := require.New(t)
	name := randomName()
	err := createDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"

	t.Run("open", func(t *testing.T) {
		idx := NewIndex(name)
		err = idx.Open()
		defer idx.Close()
		assert.NoError(err)
		assert.Empty(idx.cache, "Cache should be empty")
	})

	t.Run("put", func(t *testing.T) {
		idx := NewIndex(name)
		err := idx.Open()
		defer idx.Close()
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			expAddr := addr{pos, size}
			err := idx.Write(score, pos, size)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score])
			err = idx.Write(score, 0, 0)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score], "Should ignore update")
		}
	})

	t.Run("get", func(t *testing.T) {
		idx := NewIndex(name)
		err := idx.Open()
		defer idx.Close()
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			p, l, ok := idx.Read(score)
			assert.Equal(pos, p, "Should return correct position")
			assert.Equal(size, l, "Should return correct size")
			assert.True(ok, "Should indicate that score exists")
		}
		score := MakeScore([]byte("missing"))
		p, l, ok := idx.Read(score)
		assert.Empty(p, "Should return 0 position for missing score")
		assert.Empty(l, "Should return 0 size for missing score")
		assert.False(ok, "Should indicate that score doesn't exists")
	})

	t.Run("load", func(t *testing.T) {
		fileName := name + ".idx"
		cache, err := loadCache(fileName)
		assert.NoError(err)
		assert.Len(cache, len(strings.Fields(words)))
	})

	t.Run("close", func(t *testing.T) {
		idx := NewIndex(name)
		err := idx.Open()
		assert.NoError(err)
		assert.NotEmpty(idx.cache)
		err = idx.Close()
		assert.NoError(err)
		assert.Empty(idx.cache)
		err = idx.Close()
		assert.Error(err, "Should return error on attempt to close again")
	})

	// cleanup
	err = removeDatalogFile(name)
	assert.NoError(err)
}

// BenchmarkIndexLoad for iterative improvement of open
func BenchmarkIndexLoad(b *testing.B) {
	for n := 0; n < b.N; n++ {
		idx := NewIndex("testdata/words")
		err := idx.Open()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestIndexRebuild to ensure we can rebuild an index from a datalog
func TestIndexRebuild(t *testing.T) {
	// prepare datalog
	assert := require.New(t)
	words := "Pack my box with five dozen liquor jugs"
	name := randomName()
	err := createDatalogFile(name)
	assert.NoError(err)
	// propagate datalog
	dl := NewDatalog(name)
	err = dl.Open()
	assert.NoError(err)
	pos := intSize
	expectedCache := make(cache)
	for _, word := range strings.Fields(words) {
		data := []byte(word)
		//score := MakeScore(data)
		score, err := dl.Write(data)
		assert.NoError(err)
		size := len(data) + scoreSize
		expectedCache[score] = addr{pos, size}
		pos += size + intSize
	}
	dl.Close()
	// create an empty index and trigger rebuild by opening it
	idx := NewIndex(name)
	err = idx.Open()
	assert.NoError(err)
	assert.Equal(expectedCache, idx.cache)
	idx.Close()
	assert.Empty(idx.cache)
	// reopen index to ensure it persist
	err = idx.Open()
	assert.NoError(err)
	assert.Equal(expectedCache, idx.cache)
	// cleanup
	err = removeDatalogFile(name)
	assert.NoError(err)
}
