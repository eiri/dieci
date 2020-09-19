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
		{"lazy", 113},
		{"dog", 128},
	}

	var datalog []byte

	t.Run("Write", func(t *testing.T) {
		dr := bytes.NewReader([]byte{})
		dw := bytes.NewBuffer([]byte{})
		dl := NewDatalog(dr, dw)
		for _, tt := range datalogtests {
			data := []byte(tt.in)
			n, err := dl.Write(data)
			assert.NoError(err)
			expectedSize := intSize + scoreSize + len(data)
			assert.Equal(expectedSize, n)
			assert.Len(dw.Bytes(), tt.size)
		}
		datalog = make([]byte, dw.Len())
		copy(datalog, dw.Bytes())
	})

	t.Run("ReadAt", func(t *testing.T) {
		dr := bytes.NewReader(datalog)
		dw := bytes.NewBuffer([]byte{})
		dl := NewDatalog(dr, dw)
		for i, tt := range datalogtests {
			expectedData := []byte(tt.in)
			expectedSize := intSize + scoreSize + len(expectedData)
			data := make([]byte, expectedSize)
			off := 0
			if i > 0 {
				off = datalogtests[i-1].size
			}
			n, err := dl.ReadAt(data, int64(off))
			assert.NoError(err)
			assert.Equal(expectedSize, n)
			_, deserialized := dl.Deserialize(data)
			assert.Equal(expectedData, deserialized)
		}
	})

	t.Run("Serialize/Deserialize", func(t *testing.T) {
		dr := bytes.NewReader([]byte{})
		dw := bytes.NewBuffer([]byte{})
		dl := NewDatalog(dr, dw)
		for _, tt := range datalogtests {
			var score Score
			data := []byte(tt.in)
			serialized := dl.Serialize(score, data)
			assert.NotEqual(data, serialized)
			assert.Greater(len(serialized), len(data))
			score2, deserialized := dl.Deserialize(serialized)
			assert.Equal(data, deserialized)
			assert.Equal(score, score2)
		}
	})
}
