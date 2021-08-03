package dieci_test

import (
	"bufio"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

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
	name, err := ioutil.TempDir("", "dieci-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(name)

	w, err := os.Open("/usr/share/dict/words")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	scanner := bufio.NewScanner(w)
	scanner.Split(bufio.ScanWords)

	ds, err := dieci.Open(name)
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if ok := scanner.Scan(); !ok {
			b.Fatal("scan done before bench")
		}
		_, err = ds.Write(scanner.Bytes())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	name, err := ioutil.TempDir("", "dieci-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(name)

	w, err := os.Open("/usr/share/dict/words")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	words := bufio.NewReader(w)

	ds, err := dieci.Open(name)
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()

	keysN := 20000
	keys := make([]dieci.Key, 0)
	for {
		if len(keys) >= keysN {
			break
		}
		word, _, err := words.ReadLine()
		if err == io.EOF {
			break
		}
		key, err := ds.Write(word)
		if err != nil {
			b.Fatal(err)
		}
		keys = append(keys, key)
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		key := keys[n%keysN]
		_, err = ds.Read(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
