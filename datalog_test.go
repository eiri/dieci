package dieci

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDataLog for compliance to Datalogger
func TestDataLog(t *testing.T) {
	assert := require.New(t)
	name := RandomName()
	err := CreateDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"
	var index []byte

	t.Run("open", func(t *testing.T) {
		missing := RandomName()
		irw := bytes.NewBuffer([]byte{})
		dl, err := NewDatalog(missing, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.Error(err)
		dl, err = NewDatalog(name, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.NoError(err)
		defer dl.Close()
	})

	t.Run("write", func(t *testing.T) {
		irw := bytes.NewBuffer([]byte{})
		dl, err := NewDatalog(name, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.NoError(err)
		defer dl.Close()
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			expectedScore := MakeScore(data)
			score, err := dl.Write(data)
			assert.NoError(err)
			assert.Equal(expectedScore, score)
		}
		index = make([]byte, irw.Len())
		copy(index, irw.Bytes())
	})

	t.Run("read", func(t *testing.T) {
		tmp := make([]byte, len(index))
		copy(tmp, index)
		irw := bytes.NewBuffer(tmp)
		dl, err := NewDatalog(name, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.NoError(err)
		defer dl.Close()
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			score := MakeScore(expectedData)
			data, err := dl.Read(score)
			assert.NoError(err)
			assert.Equal(expectedData, data)
		}
	})

	t.Run("rebuild index", func(t *testing.T) {
		irw := bytes.NewBuffer([]byte{})
		dl, err := NewDatalog(name, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.NoError(err)
		defer dl.Close()
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			score := MakeScore(expectedData)
			data, err := dl.Read(score)
			assert.NoError(err)
			assert.Equal(expectedData, data)
		}
	})

	t.Run("close", func(t *testing.T) {
		irw := bytes.NewBuffer([]byte{})
		dl, err := NewDatalog(name, irw)
		assert.NoError(err)
		err = dl.Open()
		assert.NoError(err)
		err = dl.Close()
		assert.NoError(err)
		err = dl.Close()
		assert.Error(err, "Should return error on attempt to close again")
	})

	err = removeDatalogFile(name)
	assert.NoError(err)
}

// BenchmarkRebuildIndex isolated
func BenchmarkRebuildIndex(b *testing.B) {
	// open data file
	name := "testdata/words"
	reader, err := os.Open(name + ".data")
	if err != nil {
		b.Fatal(err)
	}
	dl := &Datalog{name: name, reader: reader}
	for n := 0; n < b.N; n++ {
		// create an empty index and set it to datalog
		idxName := RandomName()
		idxF, err := os.Create(idxName + ".idx")
		if err != nil {
			b.Fatal(err)
		}
		idx, err := NewIndex(idxF)
		if err != nil {
			b.Fatal(err)
		}
		dl.index = idx
		// isolated test
		b.ResetTimer()
		err = dl.RebuildIndex()
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if len(idx.cache) != 235886 {
			b.Fatal("expected index cache to be fully propagated")
		}
		idxF.Close()
		os.Remove(idxName + ".idx")
	}
	dl.Close()
}

func removeDatalogFile(name string) error {
	return os.Remove(name + ".data")
}
