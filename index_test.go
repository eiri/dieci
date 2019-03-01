package dieci

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	assert := require.New(t)
	name := randomName()
	err := prepareDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"

	t.Run("open", func(t *testing.T) {
		missing := randomName()
		_, err := openIndex(missing)
		assert.Error(err)
		// cleanup empty index
		os.Remove(missing + ".idx")
		idx, err := openIndex(name)
		defer idx.close()
		assert.NoError(err)
		assert.Empty(idx.cache, "Cache should be empty")
	})

	t.Run("put", func(t *testing.T) {
		idx, err := openIndex(name)
		defer idx.close()
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			expAddr := addr{pos, size}
			err := idx.put(score, pos, size)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score])
			err = idx.put(score, 0, 0)
			assert.NoError(err)
			assert.Equal(expAddr, idx.cache[score], "Should ignore update")
		}
	})

	t.Run("get", func(t *testing.T) {
		idx, err := openIndex(name)
		defer idx.close()
		assert.NoError(err)
		for pos, word := range strings.Fields(words) {
			data := []byte(word)
			size := len(data)
			score := MakeScore(data)
			p, l, ok := idx.get(score)
			assert.Equal(pos, p, "Should return correct position")
			assert.Equal(size, l, "Should return correct size")
			assert.True(ok, "Should indicate that score exists")
		}
		score := MakeScore([]byte("missing"))
		p, l, ok := idx.get(score)
		assert.Empty(p, "Should return 0 position for missing score")
		assert.Empty(l, "Should return 0 size for missing score")
		assert.False(ok, "Should indicate that score doesn't exists")
	})

	t.Run("load", func(t *testing.T) {
		fileName := name + ".idx"
		cache, err := loadIndex(fileName)
		assert.NoError(err)
		assert.Len(cache, len(strings.Fields(words)))
	})

	t.Run("close", func(t *testing.T) {
		idx, err := openIndex(name)
		assert.NoError(err)
		assert.NotEmpty(idx.cache)
		err = idx.close()
		assert.NoError(err)
		assert.Empty(idx.cache)
		err = idx.close()
		assert.Error(err, "Should return error on attempt to close again")
	})

	t.Run("delete", func(t *testing.T) {
		idx, err := openIndex(name)
		assert.NoError(err)
		err = idx.delete()
		assert.NoError(err)
		assert.Empty(idx.cache)
		err = idx.delete()
		assert.Error(err, "Should return error on attempt of second delete")
	})

	// cleanup
	os.Remove(name + ".data")
}

// BenchmarkIndexLoad for iterative improvement of open
func BenchmarkIndexLoad(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := loadIndex("testdata/words.idx")
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
	err := prepareDatalogFile(name)
	assert.NoError(err)
	// propagate datalog
	dl, err := openDataLog(name)
	assert.NoError(err)
	expectedCache := make(cache)
	for _, word := range strings.Fields(words) {
		data := []byte(word)
		pos, size, err := dl.put(data)
		assert.NoError(err)
		score := MakeScore(data)
		expectedCache[score] = addr{pos, size}
	}
	dl.close()
	// create and rebuild an empty index
	f, err := os.Create(name + ".idx")
	c := make(cache)
	idx := &index{c, f}
	err = rebuildIndex(name, idx)
	assert.NoError(err)
	assert.Equal(expectedCache, idx.cache)
	idx.close()
	assert.Empty(idx.cache)
	// reopen index to ensure it persist
	idx, err = openIndex(name)
	assert.NoError(err)
	assert.Equal(expectedCache, idx.cache)
	// cleanup
	dl, err = openDataLog(name)
	err = dl.delete()
	assert.NoError(err)
	err = idx.delete()
	assert.NoError(err)
}
