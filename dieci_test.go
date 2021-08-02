package dieci_test

import (
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"

	"github.com/eiri/dieci"
	"github.com/stretchr/testify/require"
)

// TestDieci basic operations
func TestDieci(t *testing.T) {
	values := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
		[]byte("echo"),
		[]byte("foxtrot"),
		[]byte("golf"),
		[]byte("hotel"),
	}

	keys := make([]dieci.Key, len(values))

	assert := require.New(t)

	name, err := ioutil.TempDir("", "dieci-test")
	assert.NoError(err)
	defer os.RemoveAll(name)

	t.Run("Open", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		ds.Close()
	})

	t.Run("Write", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		defer ds.Close()
		for i, value := range values {
			key, err := ds.Write(value)
			assert.NoError(err)
			keys[i] = key
		}
	})

	t.Run("Read", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		defer ds.Close()
		for i, key := range keys {
			value, err := ds.Read(key)
			assert.NoError(err)
			assert.Equal(values[i], value)
		}
	})
}

// BenchmarkWrite for control on writes
func BenchmarkWrite(b *testing.B) {
	b.StopTimer()
	name, err := ioutil.TempDir("", "dieci-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(name)

	ds, err := dieci.Open(name)
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	for n := 0; n < b.N; n++ {
		docSize := 1024 * 1024
		doc := make([]byte, docSize)
		_, err = rand.Read(doc)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = ds.Write(doc)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}
