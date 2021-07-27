// Package dieci implements basic API for Dieci data store
package dieci

import (
	"encoding/binary"

	"github.com/cespare/xxhash"
	badger "github.com/dgraph-io/badger/v3"
)

const (
	intSize = 8
)

type Score []byte

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
func (s *Store) Read(score Score) ([]byte, error) {
	data := make([]byte, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(score)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			data = append(data, val...)
			return nil
		})
		return err
	})
	return data, err
}

// Write given data and return it's score
func (s *Store) Write(data []byte) (Score, error) {
	h := xxhash.Sum64(data)
	score := make([]byte, intSize)
	binary.BigEndian.PutUint64(score, h)
	err := s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(score, data)
		err := txn.SetEntry(e)
		return err
	})
	if err != nil {
		return []byte{}, err
	}
	return score, nil
}

// Close provided storage
func (s *Store) Close() error {
	return s.db.Close()
}
