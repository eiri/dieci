package dieci

import (
	"bytes"
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

	t.Run("write", func(t *testing.T) {
		dw := bytes.NewBuffer([]byte{})
		dr := bytes.NewReader([]byte{})
		irw := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(irw)
		assert.NoError(err)
		dl := NewDatalog(dr, dw, idx)

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
		idx, err := NewIndex(irw)
		assert.NoError(err)
		dl := NewDatalog(dr, dw, idx)

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
		idx, err := NewIndex(irw)
		assert.NoError(err)

		dr := bytes.NewReader(datalog)
		err = idx.Rebuild(dr)
		assert.NoError(err)

		for _, word := range strings.Fields(words) {
			score := MakeScore([]byte(word))
			_, ok := idx.Read(score)
			assert.True(ok)
		}
	})
}
