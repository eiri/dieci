package dieci

import (
	badger "github.com/dgraph-io/badger/v3"
)

// datalog represents a datastore's datalog
type datalog struct {
	txn *badger.Txn
}

// newDatalog returns a new datalog for a given transaction
func newDatalog(txn *badger.Txn) *datalog {
	return &datalog{txn: txn}
}

// read is a read callback
func (dl *datalog) read(sc score) ([]byte, error) {
	data := make([]byte, 0)
	item, err := dl.txn.Get(sc)
	if err != nil {
		return data, err
	}

	err = item.Value(func(val []byte) error {
		data = append(data, val...)
		return nil
	})
	return data, err
}

// write is a write callback
func (dl *datalog) write(data []byte) (score, error) {
	sc := newScore(data)
	_, err := dl.txn.Get(sc)
	if err == nil {
		return sc, nil
	}

	e := badger.NewEntry(sc, data)
	err = dl.txn.SetEntry(e)
	if err != nil {
		return score{}, err
	}
	return sc, nil
}
