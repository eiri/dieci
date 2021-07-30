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
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		b := NewBadgerBackend(txn)
		idx := NewIndex(b)
		sc, err := idx.read(key)
		if err != nil {
			return err
		}
		dl := newDatalog(txn)
		data, err = dl.read(sc)
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
		dl := newDatalog(txn)
		sc, err := dl.write(data)
		if err != nil {
			return err
		}
		idx := NewIndex(b)
		key, err = idx.write(sc)
		return err
	})
	return key, err
}

// Close provided storage
func (s *Store) Close() error {
	return s.db.Close()
}
