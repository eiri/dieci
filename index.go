package dieci

import (
	"encoding/hex"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/muyo/sno"
	art "github.com/plar/go-adaptive-radix-tree"
)

// key is an alias for key representaion
type key []byte

func newKey() key {
	return sno.New(0).Bytes()
}

func (k key) String() string {
	return hex.EncodeToString(k)
}

// cache is in memory lookup store
type cache art.Tree

// index represents an index of a datalog file
type index struct {
	cache cache
	gen   *sno.Generator
	txn   *badger.Txn
}

// newIndex returns a new index
func newIndex(txn *badger.Txn) *index {
	cache := art.New()
	gen, err := sno.NewGenerator(&sno.GeneratorSnapshot{
		Partition: sno.Partition{0, 0},
	}, nil)
	if err != nil {
		panic(err)
	}

	return &index{cache: cache, gen: gen, txn: txn}
}

// read is a read callback
func (i *index) read(k key) (score, error) {
	if sc, ok := i.cache.Search(art.Key(k)); ok {
		return sc.(score), nil
	}

	var sc score
	item, err := i.txn.Get(k)
	if err != nil {
		return sc, err
	}
	err = item.Value(func(val []byte) error {
		sc = append(sc, val...)
		return nil
	})
	if err == nil {
		i.cache.Insert(art.Key(k), sc)
	}
	return sc, err
}

// write is a write callback
func (i *index) write(sc score) (key, error) {
	k := newKey()
	e := badger.NewEntry(k, sc)
	err := i.txn.SetEntry(e)
	if err != nil {
		return key{}, err
	}
	i.cache.Insert(art.Key(k), sc)
	return k, nil
}
