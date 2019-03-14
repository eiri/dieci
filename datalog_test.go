package dieci

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDataLog for compliance to Datalogger
func TestDataLog(t *testing.T) {
	assert := require.New(t)
	name := randomName()
	err := createDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"

	t.Run("open", func(t *testing.T) {
		missing := randomName()
		dl := NewDatalog(missing)
		err := dl.Open()
		assert.Error(err)
		dl = NewDatalog(name)
		err = dl.Open()
		assert.NoError(err)
		defer dl.Close()
		assert.Equal(0, dl.cur, "Cursor should be on 0")
	})

	t.Run("write", func(t *testing.T) {
		dl := NewDatalog(name)
		err := dl.Open()
		assert.NoError(err)
		defer dl.Close()
		expectedPos := intSize
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			score := MakeScore(data)
			pos, size, err := dl.Write(score, data)
			assert.NoError(err)
			assert.Equal(expectedPos, pos, "Position should move")
			assert.Equal(pos+size, dl.cur, "Cursor should move")
			expectedPos += size + intSize
		}
	})

	t.Run("read", func(t *testing.T) {
		dl := NewDatalog(name)
		err := dl.Open()
		assert.NoError(err)
		defer dl.Close()
		stat, err := dl.rwc.Stat()
		assert.NoError(err)
		end := int(stat.Size())
		assert.EqualValues(end, dl.cur, "Cursor should be at EOF")
		pos := 0
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			pos += intSize
			size := len(expectedData) + scoreSize
			data, err := dl.Read(pos, size)
			assert.NoError(err)
			assert.Equal(expectedData, data)
			pos += size
		}
	})

	t.Run("close", func(t *testing.T) {
		dl := NewDatalog(name)
		err := dl.Open()
		assert.NoError(err)
		defer dl.Close()
		stat, err := dl.rwc.Stat()
		assert.NoError(err)
		end := int(stat.Size())
		assert.Equal(end, dl.cur, "Cursor should be at EOF")
		err = dl.Close()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "Cursor should reset")
		err = dl.Close()
		assert.Error(err, "Should return error on attempt to close again")
	})

	err = removeDatalogFile(name)
	assert.NoError(err)
}

func createDatalogFile(name string) error {
	f, err := os.Create(fmt.Sprintf("%s.data", name))
	defer f.Close()
	return err
}

func removeDatalogFile(name string) error {
	return os.Remove(fmt.Sprintf("%s.data", name))
}

func randomName() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}
