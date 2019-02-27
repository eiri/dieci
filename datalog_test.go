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
	name, err := prepareDatalogFile()
	assert.NoError(err)

	words := "The quick brown fox jumps over the lazy dog"

	t.Run("open", func(t *testing.T) {
		dl, err := openDataLog(name)
		defer dl.close()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "expecting cursor to be at 0")
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
			assert.Equal(expectedPos, pos, "expecting position to move")
			assert.Equal(pos+size, dl.cur, "expecting cursor to move")
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
		assert.EqualValues(end, dl.cur, "expecting cursor to be at file end")
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
		assert.Equal(end, dl.cur, "expecting cursor to be at file end")
		err = dl.close()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "expecting cursor to reset")
		err = dl.close()
		assert.Error(err, "expecting an error on a second close")
	})

	t.Run("delete", func(t *testing.T) {
		dl, err := openDataLog(name)
		assert.NoError(err)
		err = dl.delete()
		assert.NoError(err)
		assert.Equal(0, dl.cur, "expecting cursor to reset")
		err = dl.delete()
		assert.Error(err, "expecting an error on a second delete")
	})
}

func prepareDatalogFile() (string, error) {
	buf := makeFakeScore()
	name := hex.EncodeToString(buf)
	// and touch datalog file
	f, err := os.Create(fmt.Sprintf("%s.data", name))
	defer f.Close()
	return name, err
}

func makeFakeScore() []byte {
	buf := make([]byte, 16)
	rand.Read(buf)
	return buf
}
