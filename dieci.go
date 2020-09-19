// Package dieci implements basic API for Dieci data store
package dieci

import (
	"fmt"
	"os"
)

// Store represents a data store.
type Store struct {
	name  string
	data  *Datalog
	index *Index
	dr    *os.File
	dw    *os.File
}

// Open opens provided storage
func Open(name string) (s *Store, err error) {
	datalogName := fmt.Sprintf("%s.data", name)
	dr, err := os.OpenFile(datalogName, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	idx := NewIndex()
	if err := idx.Load(dr); err != nil {
		dr.Close()
		return s, err
	}
	dw, err := os.OpenFile(datalogName, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		dr.Close()
		return
	}
	data := NewDatalog(dr, dw)
	s = &Store{
		name:  name,
		data:  data,
		index: idx,
		dr:    dr,
		dw:    dw,
	}
	return
}

// Read a data for a given score
func (s *Store) Read(score Score) ([]byte, error) {
	addr, ok := s.index.Get(score)
	if !ok {
		err := fmt.Errorf("dieci: unknown score %s", score)
		return nil, err
	}
	buf := make([]byte, addr.size)
	if _, err := s.data.ReadAt(buf, int64(addr.pos)); err != nil {
		return nil, err
	}
	check, data := s.data.Deserialize(buf)
	if check != score {
		err := fmt.Errorf("dieci: checksum failure")
		return nil, err
	}
	return data, nil
}

// Write given data and return it's score
func (s *Store) Write(data []byte) (Score, error) {
	score := s.data.Score(data)
	if _, ok := s.index.Get(score); ok {
		return score, nil
	}
	size, err := s.data.Write(data)
	if err != nil {
		return score, err
	}
	s.index.Put(score, size)
	return score, nil
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
