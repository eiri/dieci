// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

// Store represents a data store.
type Store struct {
	name  string
	data  datalogger
	index indexer
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
	data, err := openDataLog(name)
	if err != nil {
		return
	}
	idx, err := openIndex(name)
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
	p, l, ok := s.index.get(score)
	if !ok {
		err = fmt.Errorf("Unknown score %s", score)
		return
	}
	b, err = s.data.get(p, l)
	if score != MakeScore(b) {
		b = nil
		err = fmt.Errorf("Checksum failure")
	}
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	score = MakeScore(b)
	if _, _, ok := s.index.get(score); ok {
		return
	}
	p, l, err := s.data.put(b)
	if err != nil {
		return
	}
	err = s.index.put(score, p, l)
	if err != nil {
		return
	}
	return
}

// Close provided storage
func (s *Store) Close() error {
	if err := s.index.close(); err != nil {
		return err
	}
	return s.data.close()
}

// Delete provided storage
func (s *Store) Delete() error {
	if err := s.index.delete(); err != nil {
		return err
	}
	return s.data.delete()
}
