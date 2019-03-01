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

func TestDataLog(t *testing.T) {
	assert := require.New(t)
	name := randomName()
	err := prepareDatalogFile(name)
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"

	t.Run("open", func(t *testing.T) {
		missing := randomName()
		_, err := openDataLog(missing)
		assert.Error(err)
		dl, err := openDataLog(name)
		defer dl.close()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "Cursor should be on 0")
	})

	t.Run("put", func(t *testing.T) {
		dl, err := openDataLog(name)
		defer dl.close()
		assert.NoError(err)
		expectedPos := intSize
		for _, word := range strings.Fields(words) {
			data := []byte(word)
			pos, size, err := dl.put(data)
			assert.NoError(err)
			assert.Equal(expectedPos, pos, "Position should move")
			assert.Equal(pos+size, dl.cur, "Cursor should move")
			expectedPos += size + intSize
		}
	})

	t.Run("get", func(t *testing.T) {
		dl, err := openDataLog(name)
		defer dl.close()
		assert.NoError(err)
		i, err := dl.Stat()
		assert.NoError(err)
		end := int(i.Size())
		assert.EqualValues(end, dl.cur, "Cursor should be at EOF")
		pos := 0
		for _, word := range strings.Fields(words) {
			expectedData := []byte(word)
			pos += intSize
			size := len(expectedData)
			data, err := dl.get(pos, size)
			assert.NoError(err)
			assert.Equal(expectedData, data)
			pos += size
		}
	})

	t.Run("close", func(t *testing.T) {
		dl, err := openDataLog(name)
		assert.NoError(err)
		i, err := dl.Stat()
		assert.NoError(err)
		end := int(i.Size())
		assert.Equal(end, dl.cur, "Cursor should be at EOF")
		err = dl.close()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "Cursor should reset")
		err = dl.close()
		assert.Error(err, "Should return error on attempt to close again")
	})

	t.Run("delete", func(t *testing.T) {
		dl, err := openDataLog(name)
		assert.NoError(err)
		err = dl.delete()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "Cursor should reset")
		err = dl.delete()
		assert.Error(err, "Should return error on attempt of second delete")
	})
}

func prepareDatalogFile(name string) error {
	f, err := os.Create(fmt.Sprintf("%s.data", name))
	defer f.Close()
	return err
}

func randomName() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}
