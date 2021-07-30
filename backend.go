package dieci

import (
	badger "github.com/dgraph-io/badger/v3"
)

// Backend is an interface for Dieci backend implementation
type Backend interface {
	Read([]byte) ([]byte, error)
	Exists([]byte) (bool, error)
	Write([]byte, []byte) error
}

// BadgerBackend implements backedn with BadgerDB
type BadgerBackend struct {
	txn *badger.Txn
	err error
}

// NewBadgerBackend returns new instance of Badger backend for a given transaction
func NewBadgerBackend(txn *badger.Txn) Backend {
	return &BadgerBackend{txn: txn}
}

func (bb *BadgerBackend) Read(k []byte) ([]byte, error) {
	v := make([]byte, 0)
	item, err := bb.txn.Get(k)
	if err != nil {
		return v, err
	}
	err = item.Value(func(val []byte) error {
		v = append(v, val...)
		return nil
	})
	return v, err
}

func (bb *BadgerBackend) Exists(k []byte) (bool, error) {
	_, err := bb.txn.Get(k)
	if err == nil {
		return true, nil
	} else if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return false, err
}

func (bb *BadgerBackend) Write(k []byte, v []byte) error {
	e := badger.NewEntry(k, v)
	return bb.txn.SetEntry(e)
}
