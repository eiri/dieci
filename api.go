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
	name  string
	data  Datalogger
	index Indexer
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
	idx := NewIndex(name)
	err = idx.Open()
	if err != nil {
		return
	}
	s = &Store{
		name:  name,
		data:  data,
		index: idx,
	}
	return
}

// Name returns name of a store
func (s *Store) Name() string {
	return s.name
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	p, l, ok := s.index.Read(score)
	if !ok {
		err = fmt.Errorf("Unknown score %s", score)
		return
	}
	b, err = s.data.Read(p, l)
	if score != MakeScore(b) {
		b = nil
		err = fmt.Errorf("Checksum failure")
	}
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	score = MakeScore(b)
	if _, _, ok := s.index.Read(score); ok {
		return
	}
	p, l, err := s.data.Write(b)
	if err != nil {
		return
	}
	err = s.index.Write(score, p, l)
	if err != nil {
		return
	}
	return
}

// Close provided storage
func (s *Store) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.data.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	var err error
	idxName := s.index.Name()
	if err = s.index.Close(); err == nil {
		err = os.Remove(idxName)
	}
	dlName := s.data.Name()
	if err = s.data.Close(); err == nil {
		err = os.Remove(dlName)
	}
	return err
}