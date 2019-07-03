package dieci

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDataLog for compliance to Datalogger
func TestDataLog(t *testing.T) {
	assert := require.New(t)

	var datalogtests = []struct {
		in   string
		size int
	}{
		{"the", 15},
		{"quick", 32},
		{"brown", 49},
		{"fox", 64},
		{"jumps", 81},
		{"over", 97},
		{"the", 97},
		{"lazy", 113},
		{"dog", 128},
	}

	var datalog []byte
	var index []byte

	t.Run("write", func(t *testing.T) {
		dr := bytes.NewReader([]byte{})
		dw := bytes.NewBuffer([]byte{})
		irw := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(irw)
		assert.NoError(err)
		dl := NewDatalog(dr, dw, idx)
		for _, tt := range datalogtests {
			data := []byte(tt.in)
			expectedScore := MakeScore(data)
			score, err := dl.Write(data)
			assert.NoError(err)
			assert.Equal(expectedScore, score)
			assert.Len(dw.Bytes(), tt.size)
		}
		datalog = make([]byte, dw.Len())
		copy(datalog, dw.Bytes())
		index = make([]byte, irw.Len())
		copy(index, irw.Bytes())
	})

	t.Run("rebuild index", func(t *testing.T) {
		dr := bytes.NewReader(datalog)
		irw := bytes.NewBuffer([]byte{})
		idx, err := NewIndex(irw)
		assert.NoError(err)
		err = idx.Rebuild(dr)
		assert.NoError(err)
		assert.Equal(index, irw.Bytes())
	})

	t.Run("read", func(t *testing.T) {
		dr := bytes.NewReader(datalog)
		dw := bytes.NewBuffer([]byte{})
		irw := bytes.NewBuffer(index)
		idx, err := NewIndex(irw)
		assert.NoError(err)
		dl := NewDatalog(dr, dw, idx)
		for _, tt := range datalogtests {
			expectedData := []byte(tt.in)
			score := MakeScore(expectedData)
			data, err := dl.Read(score)
			assert.NoError(err)
			assert.Equal(expectedData, data)
		}
	})
}
