package dieci_test

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/eiri/dieci"
	"github.com/stretchr/testify/require"
)

type kv struct {
	score dieci.Score
	data  []byte
}

var storeName string
var kvs []kv

func TestAPI(t *testing.T) {
	assert := require.New(t)
	var name string
	kvs := make([]kv, 5)

	t.Run("new", func(t *testing.T) {
		ds, err := dieci.New()
		assert.NoError(err)
		name = ds.Name()
		ds.Close()
		assert.FileExists(name + ".data")
	})

	t.Run("open", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		ds.Close()
	})

	t.Run("write", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		defer ds.Close()
		for i, dataSize := range []int{2100, 1200, 4200, 500, 1700} {
			data := make([]byte, dataSize)
			_, err = rand.Read(data)
			assert.NoError(err)
			score, err := ds.Write(data)
			assert.NoError(err)
			stat, _ := os.Stat(name + ".data")
			kvs[i] = kv{score: score, data: data}
			// test deduplication
			score2, err := ds.Write(data)
			assert.NoError(err)
			stat2, _ := os.Stat(name + ".data")
			assert.Equal(score, score2, "Should return consistent score")
			assert.Equal(stat.Size(), stat2.Size())
		}
	})

	t.Run("read", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		defer ds.Close()
		for _, i := range [5]int{1, 2, 0, 4, 3} {
			kv := kvs[i]
			data, err := ds.Read(kv.score)
			assert.NoError(err)
			assert.Equal(kv.data, data)
		}
	})

	t.Run("read back", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			ds, err := dieci.Open(name)
			assert.NoError(err)
			before := make([]byte, 1024)
			rand.Read(before)
			score, err := ds.Write(before)
			assert.NoError(err)
			after, err := ds.Read(score)
			assert.NoError(err)
			assert.Equal(before, after, "Should return stored data")
			err = ds.Close()
			assert.NoError(err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		assert.FileExists(name + ".data")
		ds, err := dieci.Open(name)
		assert.NoError(err)
		err = ds.Delete()
		assert.NoError(err)
		err = ds.Delete()
		assert.Error(err, "Should return error on attempt of second delete")
		_, err = os.Stat(name + ".data")
		assert.Error(err, "Should remove store files")
	})
}

// BenchmarkOpen for iterative improvement of open
func BenchmarkOpen(b *testing.B) {
	for n := 0; n < b.N; n++ {
		s, err := dieci.Open("testdata/words")
		if err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

// BenchmarkWrite for iterative improvement or writes
func BenchmarkWrite(b *testing.B) {
	s, err := dieci.New()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		docSize := 1024
		doc := make([]byte, docSize)
		_, err = rand.Read(doc)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = s.Write(doc)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	s.Delete()

}

// BenchmarkRead for iterative improvement of reads
func BenchmarkRead(b *testing.B) {
	s, err := dieci.Open("testdata/words")
	if err != nil {
		b.Fatal(err)
	}
	score := dieci.MakeScore([]byte("witchwork"))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := s.Read(score)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	s.Close()
}
