// Package dieci implements basic API for Dieci data store
package dieci

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

// Store represents a data store.
type Store struct {
	name string
	data Datalogger
}

// New creates a new empty storage
func New() (s *Store, err error) {
	buf := make([]byte, 16)
	_, err = rand.Read(buf)
	if err != nil {
		return
	}
	storeName := hex.EncodeToString(buf)
	dataFileName := fmt.Sprintf("%s.data", storeName)
	f, err := os.OpenFile(dataFileName, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return
	}
	f.Close()
	return Open(storeName)
}

// Open opens provided storage
func Open(name string) (s *Store, err error) {
	data := NewDatalog(name)
	err = data.Open()
	if err != nil {
		return
	}
	s = &Store{
		name: name,
		data: data,
	}
	return
}

// Name returns name of a store
func (s *Store) Name() string {
	return s.name
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	b, err = s.data.Read(score)
	if score != MakeScore(b) {
		b = nil
		err = fmt.Errorf("Checksum failure")
	}
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	return s.data.Write(b)
}

// Close provided storage
func (s *Store) Close() error {
	return s.data.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	idxName := fmt.Sprintf("%s.idx", s.name)
	dlName := fmt.Sprintf("%s.data", s.name)
	err := s.data.Close()
	if err != nil {
		return err
	}
	err = os.Remove(idxName)
	if err != nil {
		return err
	}
	return os.Remove(dlName)
}
