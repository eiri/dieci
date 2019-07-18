// Package dieci implements basic API for Dieci data store
package dieci

import (
	"fmt"
	"os"
)

// Store represents a data store.
type Store struct {
	name string
	data *Datalog
	dr   *os.File
	dw   *os.File
}

// Open opens provided storage
func Open(name string) (s *Store, err error) {
	datalogName := fmt.Sprintf("%s.data", name)
	dr, err := os.OpenFile(datalogName, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	idx, err := NewIndex(dr)
	if err != nil {
		dr.Close()
		return
	}
	dw, err := os.OpenFile(datalogName, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		dr.Close()
		return
	}
	data := NewDatalog(dr, dw, idx)
	s = &Store{
		name: name,
		data: data,
		dr:   dr,
		dw:   dw,
	}
	return
}

// Read a data for a given score
func (s *Store) Read(score Score) (b []byte, err error) {
	b, err = s.data.Get(score)
	if score != MakeScore(b) {
		b = nil
		err = fmt.Errorf("dieci: checksum failure")
	}
	return
}

// Write given data and return it's score
func (s *Store) Write(b []byte) (score Score, err error) {
	return s.data.Put(b)
}

// Close provided storage
func (s *Store) Close() error {
	if err := s.dw.Close(); err != nil {
		return err
	}
	return s.dr.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.Remove(s.name + ".data")
}
