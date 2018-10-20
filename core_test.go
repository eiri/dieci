package beansdb_test

import (
	"crypto/rand"
	"github.com/eiri/beansdb"
	"os"
	"reflect"
	"testing"
)

type kv [2][]byte

var kvs []kv
var store, score string

// TestNew to ensure we can create a new storage
func TestNew(t *testing.T) {
	s, err := beansdb.New()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	store = s.Name()
	_, err = os.Stat(store)
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpen to ensure we can open an existing storage
func TestOpen(t *testing.T) {
	_, err := os.Stat(store)
	if err != nil {
		t.Fatal(err)
	}
	s, err := beansdb.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

// TestWrite to ensure we can write in the store
func TestWrite(t *testing.T) {
	kvs = make([]kv, 5)
	s, err := beansdb.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, docSize := range []int{21, 12, 42, 5, 17} {
		doc := make([]byte, docSize)
		_, err = rand.Read(doc)
		if err != nil {
			t.Fatal(err)
		}
		score, err := s.Write(doc)
		if err != nil {
			t.Fatal(err)
		}
		if len(score) != beansdb.ScoreSize {
			t.Errorf("Expecting score be %d bytes long", beansdb.ScoreSize)
		}
		kvs[i] = kv{score[:], doc}
		// double-write
	}
}

// TestRead to ensure we can read from the store
func TestRead(t *testing.T) {
	s, err := beansdb.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, i := range [5]int{1, 2, 0, 4, 3} {
		kv := kvs[i]
		var score [beansdb.ScoreSize]byte
		copy(score[:], kv[0])
		doc, err := s.Read(score)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(doc, kv[1]) {
			t.Error("Expecting store to return stored data")
		}
	}
}

// TestDelete to ensure we can delete the store
func TestDelete(t *testing.T) {
	_, err := os.Stat(store)
	if err != nil {
		t.Fatal(err)
	}
	s, err := beansdb.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	s.Delete()
	_, err = os.Stat(store)
	if !os.IsNotExist(err) {
		t.Error("Expecting store file do not exist")
	}
}
