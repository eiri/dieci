// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/rand"
	"fmt"
	"os"
)

// Store represents a data store.
type Store struct {
	*os.File
	idx map[int]int
}

// New creates a new empty storage
func New() (s Store, err error) {
	buf := make([]byte, 16)
	_, err = rand.Read(buf)
	if err != nil {
		return
	}
	f, err := os.Create(fmt.Sprintf("%x.data", buf))
	if err != nil {
		return
	}
	s = Store{f, make(map[int]int)}
	return
}

// Open opens provided storage
func Open(storeName string) (s Store, err error) {
	f, err := os.Open(storeName)
	if err != nil {
		return
	}
	idx := make(map[int]int)
	s = Store{f, idx}
	return
}

// Read a data for a given score
func Read() {
	fmt.Println("Read")
}

// Write given data and return it's score
func Write() {
	fmt.Println("Write")
}

// Delete provided storage
func (s *Store) Delete() error {
	s.Close()
	return os.Remove(s.Name())
}
