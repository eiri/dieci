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
func New() (*Store, error) {
	name := RandomName()
	if err := CreateDatalogFile(name); err != nil {
		return nil, err
	}
	return Open(name)
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

// RandomName generator for new datastores
func RandomName() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

// CreateDatalogFile with assumption it doesn't exists, essentialy `touch`
func CreateDatalogFile(name string) error {
	f, err := os.OpenFile(name+".data", os.O_CREATE|os.O_EXCL, 0600)
	defer f.Close()
	return err
}
