package dieci

import (
	art "github.com/plar/go-adaptive-radix-tree"
)

// cache is in memory lookup store
type cache art.Tree

// Index represents an index of a datalog file
type Index struct {
	cache   cache
	backend Backend
}

// NewIndex returns a new index
func NewIndex(b Backend) *Index {
	cache := art.New()
	return &Index{cache: cache, backend: b}
}

// read is a read callback
func (idx *Index) read(k key) (score, error) {
	if sc, ok := idx.cache.Search(art.Key(k)); ok {
		return sc.([]byte), nil
	}

	sc, err := idx.backend.Read(k)
	if err == nil {
		idx.cache.Insert(art.Key(k), sc)
	}
	return sc, err
}

// write is a write callback
func (idx *Index) write(sc score) (key, error) {
	k := newKey()
	err := idx.backend.Write(k, sc)
	if err != nil {
		return key{}, err
	}
	idx.cache.Insert(art.Key(k), sc)
	return k, nil
}
