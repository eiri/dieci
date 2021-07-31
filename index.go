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

// Read is a read callback
func (idx *Index) Read(key Key) (Score, error) {
	if score, ok := idx.cache.Search(art.Key(key)); ok {
		return score.([]byte), nil
	}

	score, err := idx.backend.Read(key)
	if err == nil {
		idx.cache.Insert(art.Key(key), score)
	}
	return score, err
}

// Write is a write callback
func (idx *Index) Write(score Score) (Key, error) {
	key := NewKey()
	err := idx.backend.Write(key, score)
	if err != nil {
		return Key{}, err
	}
	idx.cache.Insert(art.Key(key), score)
	return key, nil
}
