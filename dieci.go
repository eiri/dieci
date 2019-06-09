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
	irw  *os.File
}

// Open opens provided storage
func Open(name string) (s *Store, err error) {
	irw, err := os.OpenFile(name+".idx", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}
	idx, err := NewIndex(irw)
	if err != nil {
		irw.Close()
		return
	}
	datalogName := fmt.Sprintf("%s.data", name)
	dr, err := os.OpenFile(datalogName, os.O_RDONLY, 0600)
	if err != nil {
		irw.Close()
		return
	}
	if idx.Len() == 0 {
		err = idx.Rebuild(dr)
	}
	if err != nil {
		irw.Close()
		dr.Close()
		return
	}
	dw, err := os.OpenFile(datalogName, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		irw.Close()
		dr.Close()
		return
	}
	data := NewDatalog(dr, dw, idx)
	s = &Store{
		name: name,
		data: data,
		dr:   dr,
		dw:   dw,
		irw:  irw,
	}
	return
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
	if err := s.dw.Close(); err != nil {
		return err
	}
	if err := s.dr.Close(); err != nil {
		return err
	}
	return s.irw.Close()
}

// Delete provided storage
func (s *Store) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	os.Remove(s.name + ".idx")
	return os.Remove(s.name + ".data")
}
