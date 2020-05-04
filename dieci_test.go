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

var kvs []kv

func TestDieci(t *testing.T) {
	assert := require.New(t)
	name := "test"
	f, err := os.Create(name + ".data")
	assert.NoError(err)
	f.Close()

	kvs := make([]kv, 5)

	t.Run("Open", func(t *testing.T) {
		ds, err := dieci.Open(name)
		assert.NoError(err)
		ds.Close()
	})

	t.Run("Write", func(t *testing.T) {
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

	t.Run("Read", func(t *testing.T) {
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

	t.Run("Write/Read", func(t *testing.T) {
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

	t.Run("Delete", func(t *testing.T) {
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
	b.StopTimer()
	f, err := os.Create("test.data")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()
	ds, err := dieci.Open("test")
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Delete()
	for n := 0; n < b.N; n++ {
		docSize := 1024
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
