package dieci

import (
	badger "github.com/dgraph-io/badger/v3"
	"github.com/muyo/sno"
)

// key is an alias for sno's ID
type key sno.ID

func (k key) bytes() []byte {
	return sno.ID(k).Bytes()
}

// cache is in memory lookup store
type cache map[key]score

// index represents an index of a datalog file
type index struct {
	cache cache
	gen   *sno.Generator
	txn   *badger.Txn
}

// newIndex returns a new index
func newIndex(txn *badger.Txn) *index {
	cache := make(cache)
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
	if sc, ok := i.cache[k]; ok {
		return sc, nil
	}

	var sc score
	item, err := i.txn.Get(k.bytes())
	if err != nil {
		return sc, err
	}
	err = item.Value(func(val []byte) error {
		sc = append(sc, val...)
		return nil
	})
	if err == nil {
		i.cache[k] = sc
	}
	return sc, err
}

// write is a write callback
func (i *index) write(sc score) (key, error) {
	k := i.key()
	e := badger.NewEntry(k.bytes(), sc)
	err := i.txn.SetEntry(e)
	if err != nil {
		return key{}, err
	}
	i.cache[k] = sc
	return k, nil
}

// key generates new sno ID and converts it to key struct
func (i *index) key() key {
	id := i.gen.New(0)
	return key(id)
}
