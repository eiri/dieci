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

	words := "The quick brown fox jumps over the lazy dog"
	var datalog []byte
	var index []byte

	t.Run("open", func(t *testing.T) {
		dw := bytes.NewBuffer([]byte{})
		dr := bytes.NewReader([]byte{})
		irw := bytes.NewBuffer([]byte{})

		dl := NewDatalog(dr, dw)
		err := dl.Open(irw)
		assert.NoError(err)
	})

	t.Run("write", func(t *testing.T) {
		dw := bytes.NewBuffer([]byte{})
		dr := bytes.NewReader([]byte{})
		irw := bytes.NewBuffer([]byte{})

		dl := NewDatalog(dr, dw)
		err := dl.Open(irw)
		assert.NoError(err)
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			expectedScore := MakeScore(data)
			score, err := dl.Write(data)
			assert.NoError(err)
			assert.Equal(expectedScore, score)
		}

		datalog = make([]byte, dw.Len())
		copy(datalog, dw.Bytes())
		index = make([]byte, irw.Len())
		copy(index, irw.Bytes())
	})

	t.Run("read", func(t *testing.T) {
		tmp := make([]byte, len(datalog))
		copy(tmp, datalog)
		dw := bytes.NewBuffer([]byte{})
		dr := bytes.NewReader(tmp)
		tmp = make([]byte, len(index))
		copy(tmp, index)
		irw := bytes.NewBuffer(tmp)

		dl := NewDatalog(dr, dw)
		err := dl.Open(irw)
		assert.NoError(err)
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			score := MakeScore(expectedData)
			data, err := dl.Read(score)
			assert.NoError(err)
			assert.Equal(expectedData, data)
		}
	})

	t.Run("rebuild index", func(t *testing.T) {
		tmp := make([]byte, len(datalog))
		copy(tmp, datalog)
		dw := bytes.NewBuffer([]byte{})
		dr := bytes.NewReader(tmp)
		tmp = make([]byte, len(index))
		copy(tmp, index)
		irw := bytes.NewBuffer(tmp)

		dl := NewDatalog(dr, dw)
		err := dl.Open(irw)
		assert.NoError(err)
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			score := MakeScore(expectedData)
			data, err := dl.Read(score)
			assert.NoError(err)
			assert.Equal(expectedData, data)
		}
	})
}

// BenchmarkRebuildIndex isolated
func BenchmarkRebuildIndex(b *testing.B) {
	// open data file
	name := "testdata/words"
	reader, err := os.Open(name + ".data")
	if err != nil {
		b.Fatal(err)
	}
	dl := &Datalog{reader: reader}
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
	reader.Close()
}
