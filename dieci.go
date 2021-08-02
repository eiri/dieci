// Package dieci implements basic API for Dieci data store
package dieci

import (
	badger "github.com/dgraph-io/badger/v3"
)

// Store represents a data store.
type Store struct {
	name string
	db   *badger.DB
}

// Open opens provided storage
func Open(name string) (s *Store, err error) {
	opts := badger.DefaultOptions(name)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return s, err
	}
	s = &Store{name: name, db: db}
	return
}

// Read a data for a given score
func (s *Store) Read(key []byte) ([]byte, error) {
	var data []byte
	err := ValidateKey(key)
	if err != nil {
		return data, err
	}
	err = s.db.View(func(txn *badger.Txn) error {
		var err error
		b := NewBadgerBackend(txn)
		idx := NewIndex(b)
		sc, err := idx.Read(key)
		if err != nil {
			return err
		}
		dl := NewDatalog(b)
		data, err = dl.Read(sc)
		return err
	})
	return data, err
}

// Write given data and return it's score
func (s *Store) Write(data []byte) ([]byte, error) {
	var key []byte
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		b := NewBadgerBackend(txn)
		dl := NewDatalog(b)
		sc, err := dl.Write(data)
		if err != nil {
			return err
		}
		idx := NewIndex(b)
		key, err = idx.Write(sc)
		return err
	})
	return key, err
}

// Close provided storage
func (s *Store) Close() error {
	return s.db.Close()
}
