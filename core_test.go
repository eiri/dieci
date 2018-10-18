package beansdb_test

import (
	"github.com/eiri/beansdb"
	"os"
	"testing"
)

var store, score string

// TestNew to ensure we can create new storage
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

// TestOpen to ensure we can open existing storage
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

// TestDelete to ensure we can delete existing storage
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
