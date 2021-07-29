package dieci

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/cespare/xxhash"
	badger "github.com/dgraph-io/badger/v3"
)

// scoreSize is the size of score in bytes
const scoreSize = 8

// score is a type alias for score representation
type score []byte

func newScore(data []byte) score {
	h := xxhash.Sum64(data)
	sc := make([]byte, scoreSize)
	binary.BigEndian.PutUint64(sc, h)
	return score(sc)
}

// String added to comply with Stringer interface
func (s score) String() string {
	return hex.EncodeToString(s)
}

// uint64 returns original xxhash sum64 for a given score
func (s score) uint64() uint64 {
	h := binary.BigEndian.Uint64(s)
	return h
}

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
